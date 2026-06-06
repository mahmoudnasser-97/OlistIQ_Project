from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

# ── Build SparkSession ─────────────────────────────────────────────────────
# FIXES applied vs original:
#   1. Added SimpleAWSCredentialsProvider — prevents credential-chain errors
#   2. Added multipart.size + fast.upload — prevents MinIO 400 on large files
#   3. Removed cache()/unpersist() pattern — replaced with post-write count
#      from the Delta snapshot (avoids double-scanning the source CSV)
builder = (
    SparkSession.builder
    .appName("olist-bronze-ingestion")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",         "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",       "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",       "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.impl",             "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.multipart.size",   "104857600")
    .config("spark.hadoop.fs.s3a.fast.upload",      "true")
    .config("spark.sql.shuffle.partitions",         "4")
    .config("spark.default.parallelism",            "4")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "/opt/data/"
BRONZE   = "s3a://bronze/csv/"

TABLES = {
    "orders":               {"file": "olist_orders_dataset.csv"},
    "order_items":          {"file": "olist_order_items_dataset.csv"},
    "order_payments":       {"file": "olist_order_payments_dataset.csv"},
    "order_reviews":        {"file": "olist_order_reviews_dataset.csv"},
    "customers":            {"file": "olist_customers_dataset.csv"},
    "sellers":              {"file": "olist_sellers_dataset.csv"},
    "products":             {"file": "olist_products_dataset.csv"},
    "geolocation":          {"file": "olist_geolocation_dataset.csv"},
    "category_translation": {"file": "product_category_name_translation.csv"},
}

for table_name, cfg in TABLES.items():
    print(f"\n→ Ingesting {table_name}...", flush=True)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")   
        .csv(DATA_DIR + cfg["file"])
    )

    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(cfg["file"]))
    )

    target_path = f"{BRONZE}{table_name}/"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    # FIX: count AFTER write reads from the committed Delta snapshot —
    # avoids re-scanning the CSV a second time (which the old cache pattern did)
    count = spark.read.format("delta").load(target_path).count()
    print(f"   ✓ {count:,} rows → {target_path}", flush=True)

spark.stop()
print("\n✅ Bronze ingestion complete.")
