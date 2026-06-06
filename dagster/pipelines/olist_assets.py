"""
=============================================================================
dagster/pipelines/olist_assets.py
=============================================================================
Dagster asset pipeline for the Olist data platform.

Asset graph — matches your actual spark job scripts exactly:

  [bronze_all_tables]
        │
        ▼
  [silver_tables]
        │
        ▼
  [data_quality_checks]       ← blocking gate: exits 1 on failure
        │
        ▼
  [gold_layer]                ← dims + facts + PKs/FKs in PostgreSQL
        │
        ▼
  [data_marts]                ← 9 mart_* tables for Power BI

Schedule: daily at 02:00 UTC
Dagster UI: http://localhost:3000
=============================================================================
"""

import subprocess
import psycopg2
from dagster import (
    asset,
    AssetExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RetryPolicy,
    Backoff,
    Output,
    MetadataValue,
)

# ---------------------------------------------------------------------------
# Constants — match paths used inside the spark job scripts
# ---------------------------------------------------------------------------
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
JOBS_DIR     = "/opt/spark_jobs"

PG_HOST = "postgres-dw"
PG_PORT = 5432
PG_DB   = "olist_dw"
PG_USER = "olist"
PG_PASS = "olist"


# ---------------------------------------------------------------------------
# Helper: run a spark-submit and stream logs to Dagster
# Raises RuntimeError if the process exits with non-zero code, which causes
# Dagster to mark the asset as failed and stop the run.
# ---------------------------------------------------------------------------
def spark_submit(context: AssetExecutionContext, script: str) -> None:
    cmd = [SPARK_SUBMIT, "--master", SPARK_MASTER, f"{JOBS_DIR}/{script}"]
    context.log.info(f"Submitting: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    for line in process.stdout:
        line = line.rstrip()
        if line:
            context.log.info(line)

    process.wait()

    if process.returncode != 0:
        raise RuntimeError(
            f"spark-submit failed: {script} exited with code {process.returncode}"
        )


# ---------------------------------------------------------------------------
# Helper: connect to postgres-dw and return a psycopg2 connection
# ---------------------------------------------------------------------------
def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )


# ---------------------------------------------------------------------------
# Helper: get row count from a PostgreSQL table
# ---------------------------------------------------------------------------
def pg_count(table: str) -> int:
    conn = pg_connect()
    cur  = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


# =============================================================================
# ASSET 1 — BRONZE
# Reads all 9 CSVs with inferSchema=False and writes them as Delta to MinIO.
# =============================================================================

@asset(
    group_name="bronze",
    compute_kind="pyspark",
    retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL),
    description=(
        "Reads all 9 Olist CSV files from /opt/data/ and writes them as "
        "Delta Lake tables to s3a://bronze/csv/. "
        "All columns kept as strings — typing happens in Silver."
    ),
)
def bronze_all_tables(context: AssetExecutionContext) -> Output:
    spark_submit(context, "ingest_csv_to_bronze.py")

    # Log the table names as metadata so they appear in the Dagster asset page
    tables = [
        "orders", "order_items", "order_payments", "order_reviews",
        "customers", "sellers", "products", "geolocation", "category_translation",
    ]
    return Output(
        value=None,
        metadata={
            "tables_written": MetadataValue.int(len(tables)),
            "bronze_path":    MetadataValue.text("s3a://bronze/csv/"),
            "table_list":     MetadataValue.text(", ".join(tables)),
        },
    )


# =============================================================================
# ASSET 2 — SILVER
# Cleans, types, enriches, and deduplicates all 8 Bronze tables.
# Writes 8 Silver Delta tables:
#   silver_orders, silver_order_items, silver_products, silver_reviews,
#   silver_customers, silver_sellers, silver_payments, silver_geolocation
# =============================================================================

@asset(
    group_name="silver",
    compute_kind="pyspark",
    deps=[bronze_all_tables],
    retry_policy=RetryPolicy(max_retries=1, delay=60),
    description=(
        "Transforms Bronze strings into typed, cleaned Silver Delta tables. "
        "Computes delivery metrics (handling_days, total_lead_time, "
        "delivery_status_detail), seller performance ratios, and product "
        "logistics categories."
    ),
)
def silver_tables(context: AssetExecutionContext) -> Output:
    spark_submit(context, "transform_silver.py")

    silver = [
        "silver_orders", "silver_order_items", "silver_products",
        "silver_reviews", "silver_customers", "silver_sellers",
        "silver_payments", "silver_geolocation",
    ]
    return Output(
        value=None,
        metadata={
            "tables_written": MetadataValue.int(len(silver)),
            "silver_path":    MetadataValue.text("s3a://silver/"),
            "table_list":     MetadataValue.text(", ".join(silver)),
        },
    )


# =============================================================================
# ASSET 3 — DATA QUALITY GATE
# Runs data_quality.py against all 8 Silver tables.
# The script calls sys.exit(1) on any failure → Dagster marks this asset
# failed and stops the run before Gold runs on bad data.
# =============================================================================

@asset(
    group_name="silver",
    compute_kind="pyspark",
    deps=[silver_tables],
    description=(
        "Runs data quality checks against all Silver tables. "
        "Checks: null PKs, row count minimums, value range constraints, "
        "binary flag correctness, ratio bounds. "
        "Exits non-zero on failure — blocks Gold from running on bad data."
    ),
)
def data_quality_gate(context: AssetExecutionContext) -> Output:
    spark_submit(context, "data_quality.py")
    return Output(
        value=None,
        metadata={"status": MetadataValue.text("All DQ checks passed")},
    )


# =============================================================================
# ASSET 4 — GOLD LAYER
# Builds 5 dimensions + 4 fact tables in both MinIO Gold (Delta) and
# PostgreSQL. Adds PK/FK constraints in PostgreSQL after load.
# =============================================================================

@asset(
    group_name="gold",
    compute_kind="pyspark",
    deps=[data_quality_gate],
    retry_policy=RetryPolicy(max_retries=1, delay=60),
    description=(
        "Builds the Kimball star schema in MinIO Gold and PostgreSQL:\n"
        "  Dimensions: dim_date, dim_customer, dim_product, dim_seller, dim_order_status_detail\n"
        "  Facts: fct_orders, fct_seller_fulfillment, fct_customer_payment, fct_customer_review\n"
        "Adds primary key and foreign key constraints after load."
    ),
)
def gold_layer(context: AssetExecutionContext) -> Output:
    spark_submit(context, "aggregate_gold.py")

    # Collect row counts from PostgreSQL to log as metadata
    gold_tables = [
        "dim_date", "dim_customer", "dim_product", "dim_seller",
        "dim_order_status_detail", "fct_orders",
        "fct_seller_fulfillment", "fct_customer_payment", "fct_customer_review",
    ]
    counts = {}
    for t in gold_tables:
        try:
            counts[t] = pg_count(t)
        except Exception as e:
            context.log.warning(f"Could not count {t}: {e}")
            counts[t] = -1

    total = sum(v for v in counts.values() if v > 0)
    context.log.info(f"Gold row counts: {counts}")

    return Output(
        value=None,
        metadata={
            "total_rows_loaded": MetadataValue.int(total),
            **{f"rows_{t}": MetadataValue.int(c) for t, c in counts.items()},
        },
    )


# =============================================================================
# ASSET 5 — DATA MARTS (SERVING LAYER)
# Reads from Gold PostgreSQL tables, builds 9 analytical mart_* tables
# optimised for Power BI direct connection.
# =============================================================================

@asset(
    group_name="serving",
    compute_kind="pyspark",
    deps=[gold_layer],
    retry_policy=RetryPolicy(max_retries=1, delay=30),
    description=(
        "Builds 9 Power BI-ready data marts from the Gold layer:\n"
        "  mart_sales_analytics      — revenue by month/state\n"
        "  mart_seller_performance   — seller scorecards (Q2)\n"
        "  mart_seller_alerts        — sellers >10% late in 90 days (Q4)\n"
        "  mart_product_analytics    — category revenue + delivery (Q1)\n"
        "  mart_delivery_analytics   — delivery performance by month/state\n"
        "  mart_customer_analytics   — CLV by state (Q6)\n"
        "  mart_customer_satisfaction— freight vs satisfaction (Q3)\n"
        "  mart_order_funnel         — monthly funnel (Q5)\n"
        "  mart_payment_analytics    — payment type breakdown"
    ),
)
def data_marts(context: AssetExecutionContext) -> Output:
    spark_submit(context, "load_data_marts.py")

    mart_tables = [
        "mart_sales_analytics",
        "mart_seller_performance",
        "mart_seller_alerts",
        "mart_product_analytics",
        "mart_delivery_analytics",
        "mart_customer_analytics",
        "mart_customer_satisfaction",
        "mart_order_funnel",
        "mart_payment_analytics",
    ]
    counts = {}
    for t in mart_tables:
        try:
            counts[t] = pg_count(t)
        except Exception as e:
            context.log.warning(f"Could not count {t}: {e}")
            counts[t] = -1

    context.log.info(f"Mart row counts: {counts}")

    return Output(
        value=None,
        metadata={
            "marts_created":    MetadataValue.int(len(mart_tables)),
            "powerbi_target_db": MetadataValue.text(f"postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"),
            **{f"rows_{t}": MetadataValue.int(c) for t, c in counts.items()},
        },
    )


# =============================================================================
# ASSET CHECKS — run after materialisation, visible in Dagster UI
# These supplement (not replace) the data_quality_gate asset.
# =============================================================================

@asset_check(asset=gold_layer, blocking=False)
def check_fct_orders_not_empty(context):
    """Warns if fct_orders has 0 rows — means Gold failed silently."""
    count = pg_count("fct_orders")
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"fct_orders_row_count": MetadataValue.int(count)},
    )


@asset_check(asset=gold_layer, blocking=False)
def check_dim_date_range(context):
    """Verifies dim_date covers 2016 (first Olist orders) through 2025."""
    conn = pg_connect()
    cur  = conn.cursor()
    cur.execute("SELECT MIN(year_number), MAX(year_number) FROM dim_date")
    min_yr, max_yr = cur.fetchone()
    cur.close()
    conn.close()
    passed = (min_yr is not None and min_yr <= 2016 and max_yr >= 2025)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "min_year": MetadataValue.int(min_yr or 0),
            "max_year": MetadataValue.int(max_yr or 0),
        },
    )


@asset_check(asset=data_marts, blocking=False)
def check_all_marts_populated(context):
    """Warns if any mart table has 0 rows after the serving job."""
    marts = [
        "mart_sales_analytics", "mart_seller_performance", "mart_seller_alerts",
        "mart_product_analytics", "mart_delivery_analytics",
        "mart_customer_analytics", "mart_customer_satisfaction",
        "mart_order_funnel", "mart_payment_analytics",
    ]
    empty = []
    counts = {}
    for t in marts:
        c = pg_count(t)
        counts[t] = c
        if c == 0:
            empty.append(t)

    return AssetCheckResult(
        passed=len(empty) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "empty_tables": MetadataValue.text(", ".join(empty) if empty else "none"),
            **{t: MetadataValue.int(c) for t, c in counts.items()},
        },
    )


@asset_check(asset=data_marts, blocking=False)
def check_seller_alerts_logic(context):
    """
    Verifies that mart_seller_alerts only contains sellers with
    late_delivery_rate_pct > 10 — confirms the Q4 filter is working.
    """
    conn = pg_connect()
    cur  = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM mart_seller_alerts
        WHERE late_delivery_rate_pct <= 10
    """)
    bad_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM mart_seller_alerts")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()

    return AssetCheckResult(
        passed=bad_rows == 0,
        metadata={
            "sellers_in_alert_table":       MetadataValue.int(total),
            "rows_violating_10pct_threshold": MetadataValue.int(bad_rows),
        },
    )


# =============================================================================
# JOB — groups all 5 assets into a single runnable pipeline
# =============================================================================

olist_batch_job = define_asset_job(
    name="olist_batch_pipeline",
    selection=[
        "bronze_all_tables",
        "silver_tables",
        "data_quality_gate",
        "gold_layer",
        "data_marts",
    ],
    description="Full Olist batch pipeline: Bronze → Silver → DQ → Gold → Marts",
)


# =============================================================================
# SCHEDULE — runs the full pipeline every day at 02:00 UTC
# Enable it in the Dagster UI: Automation → olist_daily_2am → toggle Running
# =============================================================================

daily_schedule = ScheduleDefinition(
    job=olist_batch_job,
    cron_schedule="0 2 * * *",
    name="olist_daily_2am",
    description="Runs the full Olist batch pipeline at 02:00 UTC daily",
)


# =============================================================================
# DEFINITIONS — the single top-level object workspace.yaml points at
# =============================================================================

defs = Definitions(
    assets=[
        bronze_all_tables,
        silver_tables,
        data_quality_gate,
        gold_layer,
        data_marts,
    ],
    asset_checks=[
        check_fct_orders_not_empty,
        check_dim_date_range,
        check_all_marts_populated,
        check_seller_alerts_logic,
    ],
    jobs=[olist_batch_job],
    schedules=[daily_schedule],
)
