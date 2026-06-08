from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

DATA_DIR = "/opt/data/"
BRONZE_BASE = "s3a://bronze/csv/"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
    "marketing_qualified_leads": "olist_marketing_qualified_leads_dataset.csv",
    "closed_deals": "olist_closed_deals_dataset.csv",
}

builder = (
    SparkSession.builder
    .appName("olist_bronze_ingestion")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def read_csv(file_name):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(DATA_DIR + file_name)
    )

for table_name, file_name in TABLES.items():
    print(f"Starting bronze ingestion for {table_name}", flush=True)
    df = read_csv(file_name)
    df = (
        df.withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(file_name))
    )
    target_path = f"{BRONZE_BASE}{table_name}/"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    print(f"Finished bronze ingestion for {table_name}. Rows: {df.count()}", flush=True)

spark.stop()
print("Bronze ingestion completed.", flush=True)
