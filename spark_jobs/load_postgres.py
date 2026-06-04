from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("olist-postgres-loader")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .getOrCreate())

G = "s3a://gold/"
jdbc_url = "jdbc:postgresql://postgres-dw:5432/olist_dw"
properties = {
    "user": "olist",
    "password": "olist",
    "driver": "org.postgresql.Driver"
}

gold_marts = [
    "mart_category_performance",
    "mart_seller_profile",
    "mart_regional_freight",
    "mart_late_sellers_90d",
    "mart_order_funnel",
    "mart_state_clv"
]

print("\n📦 Pushing to PostgreSQL...", flush=True)
for mart in gold_marts:
    print(f"Loading: {mart}...")
    df = spark.read.format("delta").load(f"{G}{mart}")
    df.write.jdbc(url=jdbc_url, table=mart, mode="overwrite", properties=properties)

print("\n🚀 ETL Pipeline Successfully Concluded.", flush=True)
spark.stop()