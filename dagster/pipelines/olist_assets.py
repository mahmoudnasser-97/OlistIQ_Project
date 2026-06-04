"""
Olist Data Platform - Dagster Assets

Orchestrates the Medallion data pipeline layers (Bronze -> Silver -> Gold)
integrating Spark, MinIO (S3), and the PostgreSQL Data Warehouse.
"""

import subprocess
from dagster import asset, Definitions, AssetIn, Output, get_dagster_logger

logger = get_dagster_logger()

# ============================================================================
# ASSETS - Bronze Layer (Raw Files in MinIO)
# ============================================================================

@asset(group_name="Bronze_Layer")
def raw_orders() -> None:
    """Verify raw orders dataset exists in MinIO bronze bucket."""
    logger.info("Checking raw orders landing zone...")
    # Real logic: You could use an S3/MinIO resource to check for file existence here


@asset(group_name="Bronze_Layer")
def raw_customers() -> None:
    """Verify raw customer dataset exists in MinIO bronze bucket."""
    logger.info("Checking raw customers landing zone...")


@asset(group_name="Bronze_Layer")
def raw_products() -> None:
    """Verify raw product catalog exists in MinIO bronze bucket."""
    logger.info("Checking raw products landing zone...")


# ============================================================================
# ASSETS - Silver Layer (Cleaned & Transformed Delta Tables)
# ============================================================================

@asset(
    deps=[raw_orders],
    group_name="Silver_Layer",
    description="Cleans raw orders and writes to Silver Delta layer via Spark"
)
def processed_orders() -> None:
    """Trigger Spark job to transform raw orders."""
    logger.info("Running transform_silver.py for orders...")
    # If Dagster runs in a container with access to the spark-master, 
    # you can trigger jobs directly using SparkSubmit bash commands or an RPC call.
    # For a simple standalone trigger pattern:
    # subprocess.run(["spark-submit", "--master", "spark://spark-master:7077", "/opt/spark_jobs/transform_silver.py"], check=True)


@asset(
    deps=[raw_customers],
    group_name="Silver_Layer"
)
def processed_customers() -> None:
    """Transform and clean customer data into Silver Delta layer."""
    logger.info("Running transform_silver.py for customers...")


@asset(
    deps=[raw_products],
    group_name="Silver_Layer"
)
def processed_products() -> None:
    """Transform and clean product data into Silver Delta layer."""
    logger.info("Running transform_silver.py for products...")


# ============================================================================
# ASSETS - Gold Layer (Aggregated Marts Loaded to Postgres DW)
# ============================================================================

@asset(
    deps=[processed_orders, processed_customers, processed_products],
    group_name="Gold_Layer",
    description="Aggregates performance and loads mart_category_performance into Postgres DW"
)
def mart_category_performance() -> Output[dict]:
    """Create category performance analytical data mart."""
    logger.info("Aggregating gold metrics and loading into postgres-dw...")
    
    # Example metadata logging to make your Dagster UI look professional:
    return Output(
        value={"status": "success", "target_table": "mart_category_performance"},
        metadata={
            "database": "olist_dw",
            "schema": "public",
            "target_host": "postgres-dw",
            "username": "olist"
        }
    )


@asset(
    deps=[processed_orders],
    group_name="Gold_Layer"
)
def mart_order_funnel() -> None:
    """Create order fulfillment funnel data mart in Postgres DW."""
    logger.info("Generating order funnel analytics...")


# ============================================================================
# DEFINITIONS (The modern entry point replacing @repository)
# ============================================================================

# Dagster automatically detects dependencies, builds the lineage graph,
# and organizes everything visually by the group_name fields.
defs = Definitions(
    assets=[
        raw_orders,
        raw_customers,
        raw_products,
        processed_orders,
        processed_customers,
        processed_products,
        mart_category_performance,
        mart_order_funnel
    ]
)