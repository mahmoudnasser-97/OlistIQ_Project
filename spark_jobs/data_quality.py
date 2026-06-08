import psycopg2
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "s3a://silver/"
GOLD = "s3a://gold/"
QA = "s3a://silver/QA_Issues/"
PG_HOST = "postgres-dw"
PG_PORT = 5432
PG_DB = "olist_dw"
PG_USER = "olist"
PG_PASSWORD = "olist"
PG_URL = "jdbc:postgresql://postgres-dw:5432/olist_dw"
PG_PROPS = {"user": "olist", "password": "olist", "driver": "org.postgresql.Driver"}

builder = (
    SparkSession.builder
    .appName("olist_integrated_data_quality")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_delta(base, name):
    return spark.read.format("delta").load(f"{base}{name}")


def save_issue(df, name):
    df = df.withColumn("dq_checked_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{QA}{name}")
    print(f"{name}: {df.count()} issue rows", flush=True)


def pg_query(query):
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


silver_customers = read_delta(SILVER, "silver_customers")
silver_sellers = read_delta(SILVER, "silver_sellers")
silver_products = read_delta(SILVER, "silver_products")
silver_orders = read_delta(SILVER, "silver_orders")
silver_items = read_delta(SILVER, "silver_order_items")
silver_payments = read_delta(SILVER, "silver_payments")
silver_reviews = read_delta(SILVER, "silver_reviews")
silver_mql = read_delta(SILVER, "silver_marketing_qualified_leads")
silver_closed = read_delta(SILVER, "silver_closed_deals")
seller_acquisition = read_delta(SILVER, "seller_acquisition_staging")
sales_staging = read_delta(SILVER, "sales_staging")
delivery_staging = read_delta(SILVER, "order_delivery_staging")
reviews_staging = read_delta(SILVER, "reviews_staging")
seller_fulfillment_staging = read_delta(SILVER, "seller_fulfillment_staging")
seller_performance_monthly_staging = read_delta(SILVER, "seller_performance_monthly_staging")

save_issue(silver_customers.filter(F.col("customer_id").isNull()), "dq_silver_customers_null_customer_id")
save_issue(silver_sellers.filter(F.col("seller_id").isNull()), "dq_silver_sellers_null_seller_id")
save_issue(silver_products.filter(F.col("product_id").isNull()), "dq_silver_products_null_product_id")
save_issue(silver_orders.filter(F.col("order_id").isNull() | F.col("customer_id").isNull()), "dq_silver_orders_null_keys")

save_issue(
    silver_items.join(silver_orders.select("order_id"), "order_id", "left_anti")
    .withColumn("dq_rule", F.lit("order_items_orphan_order_id")),
    "dq_silver_order_items_orphan_order_id"
)

save_issue(
    silver_items.join(silver_products.select("product_id"), "product_id", "left_anti")
    .withColumn("dq_rule", F.lit("order_items_orphan_product_id")),
    "dq_silver_order_items_orphan_product_id"
)

save_issue(
    silver_items.join(silver_sellers.select("seller_id"), "seller_id", "left_anti")
    .withColumn("dq_rule", F.lit("order_items_orphan_seller_id")),
    "dq_silver_order_items_orphan_seller_id"
)

save_issue(
    silver_payments.join(silver_orders.select("order_id"), "order_id", "left_anti")
    .withColumn("dq_rule", F.lit("payments_orphan_order_id")),
    "dq_silver_payments_orphan_order_id"
)

save_issue(
    silver_reviews.join(silver_orders.select("order_id"), "order_id", "left_anti")
    .withColumn("dq_rule", F.lit("reviews_orphan_order_id")),
    "dq_silver_reviews_orphan_order_id"
)

save_issue(
    silver_closed.join(silver_mql.select("mql_id"), "mql_id", "left_anti")
    .withColumn("dq_rule", F.lit("closed_deals_orphan_mql_id")),
    "dq_silver_closed_deals_orphan_mql_id"
)

save_issue(
    seller_acquisition.filter(F.col("converted_flag") == True)
    .join(silver_sellers.select("seller_id"), "seller_id", "left_anti")
    .withColumn("dq_rule", F.lit("converted_marketing_seller_not_in_silver_sellers")),
    "dq_seller_acquisition_seller_not_in_silver_sellers"
)

save_issue(
    sales_staging.filter(
        F.col("order_id").isNull() |
        F.col("order_item_id").isNull() |
        F.col("product_id").isNull() |
        F.col("seller_id").isNull() |
        F.col("customer_id").isNull()
    ).withColumn("dq_rule", F.lit("sales_staging_null_business_keys")),
    "dq_sales_staging_null_business_keys"
)

save_issue(
    delivery_staging.filter(F.col("order_id").isNull() | F.col("customer_id").isNull())
    .withColumn("dq_rule", F.lit("delivery_staging_null_business_keys")),
    "dq_delivery_staging_null_business_keys"
)

save_issue(
    reviews_staging.filter(F.col("review_id").isNull() | F.col("order_id").isNull())
    .withColumn("dq_rule", F.lit("reviews_staging_null_business_keys")),
    "dq_reviews_staging_null_business_keys"
)

save_issue(
    seller_fulfillment_staging.filter(
        F.col("order_id").isNull() |
        F.col("order_item_id").isNull() |
        F.col("seller_id").isNull() |
        F.col("product_id").isNull()
    ).withColumn("dq_rule", F.lit("seller_fulfillment_staging_null_business_keys")),
    "dq_seller_fulfillment_staging_null_business_keys"
)

save_issue(
    seller_performance_monthly_staging.filter(
        F.col("seller_id").isNull() |
        F.col("performance_year").isNull() |
        F.col("performance_month").isNull()
    ).withColumn("dq_rule", F.lit("seller_performance_monthly_null_business_keys")),
    "dq_seller_performance_monthly_null_business_keys"
)

expected_public_tables = [
    "dim_customer", "dim_seller", "dim_delivery_status", "dim_geolocation",
    "dim_date", "dim_product", "dim_review_sentiment", "dim_payment_type",
    "fct_order_sales", "fct_customer_reviews", "fct_order_delivery", "fct_seller_fulfillment",
]

expected_mart_schemas = [
    "sales_mart",
    "customer_satisfaction_mart",
    "delivery_performance_mart",
    "seller_performance_mart",
    "seller_acquisition_effectiveness_mart",
]

try:
    public_tables = set(row[0] for row in pg_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
    """))

    missing_public = [(name,) for name in expected_public_tables if name not in public_tables]
    missing_public_df = spark.createDataFrame(missing_public, ["missing_table"]) if missing_public else spark.createDataFrame([], "missing_table string")
    save_issue(missing_public_df.withColumn("dq_rule", F.lit("missing_expected_public_gold_table")), "dq_postgres_missing_public_gold_tables")

    schemas = set(row[0] for row in pg_query("""
        SELECT schema_name
        FROM information_schema.schemata
    """))

    missing_schemas = [(name,) for name in expected_mart_schemas if name not in schemas]
    missing_schemas_df = spark.createDataFrame(missing_schemas, ["missing_schema"]) if missing_schemas else spark.createDataFrame([], "missing_schema string")
    save_issue(missing_schemas_df.withColumn("dq_rule", F.lit("missing_expected_mart_schema")), "dq_postgres_missing_mart_schemas")

except Exception as exc:
    error_df = spark.createDataFrame([(str(exc),)], ["postgres_validation_error"])
    save_issue(error_df.withColumn("dq_rule", F.lit("postgres_validation_failed")), "dq_postgres_validation_error")

spark.stop()
print("Data quality validation completed.", flush=True)
