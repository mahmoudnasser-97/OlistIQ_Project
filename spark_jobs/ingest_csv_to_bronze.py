from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

# ── Build SparkSession ─────────────────────────────────────────────────────
builder = (
    SparkSession.builder
    .appName("olist-bronze-ingestion")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Optimize Spark for S3A writing
    .config("spark.hadoop.fs.s3a.endpoint",        "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",      "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",      "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.impl",            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Performance tweaks for local Docker clusters
    .config("spark.sql.shuffle.partitions",        "4") 
    .config("spark.default.parallelism",           "4")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "/opt/data/"
BRONZE   = "s3a://bronze/csv/"

TABLES = {
    "orders": {"file": "olist_orders_dataset.csv"},
    "order_items": {"file": "olist_order_items_dataset.csv"},
    "order_payments": {"file": "olist_order_payments_dataset.csv"},
    "order_reviews": {"file": "olist_order_reviews_dataset.csv"},
    "customers": {"file": "olist_customers_dataset.csv"},
    "sellers": {"file": "olist_sellers_dataset.csv"},
    "products": {"file": "olist_products_dataset.csv"},
    "geolocation": {"file": "olist_geolocation_dataset.csv"},
    "category_translation": {"file": "product_category_name_translation.csv"},
}

# ── Ingest each table ──────────────────────────────────────────────────────
for table_name, cfg in TABLES.items():
    print(f"\n→ Ingesting {table_name}...", flush=True)

    # 1. FAST READ: inferSchema=False. Read everything as strings instantly.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false") 
        .csv(DATA_DIR + cfg["file"])
    )

    # 2. Add ingestion metadata columns 
    # (Move timestamp conversions to your Silver Layer script!)
    df = (
        df
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.lit(cfg["file"]))
    )

    # 3. Cache the execution plan so .count() doesn't trigger a re-read
    df = df.cache()

    # 4. Write to Delta
    target_path = f"{BRONZE}{table_name}/"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    # Now count is pulling safely from memory cache
    count = df.count()
    print(f"   ✓ {count:,} rows → {target_path}", flush=True)
    
    # Clean up memory cache for the next table loop
    df.unpersist()

spark.stop()
print("\n✅ Bronze ingestion complete.")