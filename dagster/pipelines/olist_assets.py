"""
Olist Data Platform – Dagster Pipeline
=======================================
Orchestrates the full medallion ETL:
  Bronze  → transform_silver → Silver
  Silver  → aggregate_gold   → Gold (Delta + Postgres public schema)
  Gold    → load_data_marts  → Mart schemas in Postgres
  Silver/Gold → data_quality → QA issues in s3a://silver/QA_Issues/

All Spark jobs are submitted to the standalone Spark cluster via
spark-submit (subprocess), matching the existing docker-compose setup.
"""

import subprocess
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    RetryPolicy,
    Backoff,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
JOBS_DIR = "/opt/spark_jobs"

DELTA_PACKAGES = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

PG_PACKAGE = "org.postgresql:postgresql:42.7.2"

S3A_CONF = [
    "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
    "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
    "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
    "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
]

DELTA_CONF = [
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]


def _submit(context: AssetExecutionContext, script: str, extra_packages: str = "") -> None:
    """Run a Spark job and stream its logs to Dagster."""
    packages = DELTA_PACKAGES
    if extra_packages:
        packages = f"{packages},{extra_packages}"

    cmd = [
        SPARK_SUBMIT,
        "--master", SPARK_MASTER,
        "--packages", packages,
        *DELTA_CONF,
        *S3A_CONF,
        "--conf", "spark.sql.shuffle.partitions=4",
        f"{JOBS_DIR}/{script}",
    ]

    context.log.info("Submitting: %s", " ".join(cmd))

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as proc:
        for line in proc.stdout:
            context.log.info(line.rstrip())
        proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(
            f"spark-submit exited with code {proc.returncode} for {script}"
        )


# ---------------------------------------------------------------------------
# Assets – one per pipeline stage
# ---------------------------------------------------------------------------

@asset(
    group_name="olist_etl",
    description="Ingest raw CSVs from /opt/data into the Bronze Delta layer (s3a://bronze/).",
    retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL),
)
def bronze_ingestion(context: AssetExecutionContext) -> None:
    _submit(context, "ingest_csv_to_bronze.py")


@asset(
    group_name="olist_etl",
    description=(
        "Clean, cast, and enrich Bronze tables into Silver Delta tables "
        "(s3a://silver/). Writes QA audit logs for any bad rows."
    ),
    deps=[bronze_ingestion],
    retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL),
)
def silver_transformation(context: AssetExecutionContext) -> None:
    _submit(context, "transform_silver.py")


@asset(
    group_name="olist_etl",
    description=(
        "Build Gold-layer dimensional model (dims + facts) from Silver staging "
        "tables. Writes to s3a://gold/ and the Postgres public schema."
    ),
    deps=[silver_transformation],
    retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL),
)
def gold_aggregation(context: AssetExecutionContext) -> None:
    _submit(context, "aggregate_gold.py", extra_packages=PG_PACKAGE)


@asset(
    group_name="olist_etl",
    description=(
        "Load Gold tables into mart-specific Postgres schemas "
        "(sales_mart, delivery_performance_mart, …) and into s3a://gold/marts/."
    ),
    deps=[gold_aggregation],
    retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL),
)
def data_mart_load(context: AssetExecutionContext) -> None:
    _submit(context, "load_data_marts.py", extra_packages=PG_PACKAGE)


@asset(
    group_name="olist_etl",
    description=(
        "Run integrated data-quality checks across Silver staging tables and "
        "Postgres Gold tables. Issues are written to s3a://silver/QA_Issues/."
    ),
    # DQ runs after marts so it can also validate Postgres schema presence.
    deps=[data_mart_load],
    retry_policy=RetryPolicy(max_retries=1, delay=15),
)
def data_quality_checks(context: AssetExecutionContext) -> None:
    _submit(context, "data_quality.py", extra_packages=PG_PACKAGE)


# ---------------------------------------------------------------------------
# Job + Schedule
# ---------------------------------------------------------------------------

olist_full_pipeline = define_asset_job(
    name="olist_full_pipeline",
    selection=AssetSelection.groups("olist_etl"),
    description="End-to-end Olist ETL: Bronze → Silver → Gold → Marts → DQ",
)

olist_daily_schedule = ScheduleDefinition(
    name="olist_daily_schedule",
    job=olist_full_pipeline,
    cron_schedule="0 3 * * *",   # 03:00 UTC every day
    description="Trigger the full Olist pipeline daily at 03:00 UTC",
)

# ---------------------------------------------------------------------------
# Definitions (entry-point consumed by Dagster's workspace.yaml)
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
        bronze_ingestion,
        silver_transformation,
        gold_aggregation,
        data_mart_load,
        data_quality_checks,
    ],
    jobs=[olist_full_pipeline],
    schedules=[olist_daily_schedule],
)
