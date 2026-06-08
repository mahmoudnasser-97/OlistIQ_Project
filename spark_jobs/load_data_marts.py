import psycopg2
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

GOLD = "s3a://gold/"
MART_BASE = "s3a://gold/marts/"
PG_URL = "jdbc:postgresql://postgres-dw:5432/olist_dw"
PG_PROPS = {"user": "olist", "password": "olist", "driver": "org.postgresql.Driver"}

MARTS = {
    "sales_mart": {
        "dimensions": ["dim_product", "dim_customer", "dim_seller", "dim_date", "dim_payment_type"],
        "facts": ["fct_order_sales"],
        "primary_keys": {
            "dim_product": "product_sk",
            "dim_customer": "customer_sk",
            "dim_seller": "seller_sk",
            "dim_date": "date_sk",
            "dim_payment_type": "payment_type_sk",
            "fct_order_sales": "sales_fact_sk",
        },
        "foreign_keys": [
            ("fct_order_sales", "product_sk_fk", "dim_product", "product_sk"),
            ("fct_order_sales", "customer_sk_fk", "dim_customer", "customer_sk"),
            ("fct_order_sales", "seller_sk_fk", "dim_seller", "seller_sk"),
            ("fct_order_sales", "sales_date_sk", "dim_date", "date_sk"),
            ("fct_order_sales", "payment_type_sk_fk", "dim_payment_type", "payment_type_sk"),
        ],
    },
    "delivery_performance_mart": {
        "dimensions": ["dim_customer", "dim_seller", "dim_date", "dim_delivery_status"],
        "facts": ["fct_order_delivery"],
        "primary_keys": {
            "dim_customer": "customer_sk",
            "dim_seller": "seller_sk",
            "dim_date": "date_sk",
            "dim_delivery_status": "delivery_status_sk",
            "fct_order_delivery": "delivery_fact_sk",
        },
        "foreign_keys": [
            ("fct_order_delivery", "customer_sk_fk", "dim_customer", "customer_sk"),
            ("fct_order_delivery", "seller_sk_fk", "dim_seller", "seller_sk"),
            ("fct_order_delivery", "purchase_date_sk", "dim_date", "date_sk"),
            ("fct_order_delivery", "delivery_status_sk_fk", "dim_delivery_status", "delivery_status_sk"),
        ],
    },
    "customer_satisfaction_mart": {
        "dimensions": ["dim_seller", "dim_date", "dim_review_sentiment"],
        "facts": ["fct_customer_reviews"],
        "primary_keys": {
            "dim_seller": "seller_sk",
            "dim_date": "date_sk",
            "dim_review_sentiment": "review_sentiment_sk",
            "fct_customer_reviews": "review_fact_sk",
        },
        "foreign_keys": [
            ("fct_customer_reviews", "seller_sk_fk", "dim_seller", "seller_sk"),
            ("fct_customer_reviews", "review_date_sk", "dim_date", "date_sk"),
            ("fct_customer_reviews", "review_sentiment_sk_fk", "dim_review_sentiment", "review_sentiment_sk"),
        ],
    },
    "seller_performance_mart": {
        "dimensions": ["dim_seller", "dim_product", "dim_date"],
        "facts": ["fct_seller_fulfillment"],
        "primary_keys": {
            "dim_seller": "seller_sk",
            "dim_product": "product_sk",
            "dim_date": "date_sk",
            "fct_seller_fulfillment": "fulfillment_fact_sk",
        },
        "foreign_keys": [
            ("fct_seller_fulfillment", "seller_sk_fk", "dim_seller", "seller_sk"),
            ("fct_seller_fulfillment", "product_sk_fk", "dim_product", "product_sk"),
            ("fct_seller_fulfillment", "purchase_date_sk", "dim_date", "date_sk"),
        ],
    },
    "seller_acquisition_effectiveness_mart": {
        "dimensions": ["dim_seller", "dim_product", "dim_date"],
        "facts": ["fct_seller_fulfillment"],
        "primary_keys": {
            "dim_seller": "seller_sk",
            "dim_product": "product_sk",
            "dim_date": "date_sk",
            "fct_seller_fulfillment": "fulfillment_fact_sk",
        },
        "foreign_keys": [
            ("fct_seller_fulfillment", "seller_sk_fk", "dim_seller", "seller_sk"),
            ("fct_seller_fulfillment", "product_sk_fk", "dim_product", "product_sk"),
            ("fct_seller_fulfillment", "purchase_date_sk", "dim_date", "date_sk"),
        ],
    },
}

builder = (
    SparkSession.builder
    .appName("olist_data_marts")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def pg_exec(statements):
    conn = psycopg2.connect(host="postgres-dw", port=5432, dbname="olist_dw", user="olist", password="olist")
    conn.autocommit = True
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    cur.close()
    conn.close()

def read_gold(table):
    return spark.read.format("delta").load(f"{GOLD}{table}")

def save_mart_table(schema, table):
    df = read_gold(table)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{MART_BASE}{schema}/{table}")
    df.write.jdbc(url=PG_URL, table=f"{schema}.{table}", mode="overwrite", properties=PG_PROPS)
    print(f"Saved {schema}.{table}. Rows: {df.count()}", flush=True)

for schema, definition in MARTS.items():
    pg_exec([f"DROP SCHEMA IF EXISTS {schema} CASCADE", f"CREATE SCHEMA {schema}"])
    for table in definition["dimensions"] + definition["facts"]:
        save_mart_table(schema, table)

    constraints = []
    for table, pk in definition["primary_keys"].items():
        constraints.append(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({pk})")
    for fact, fk_col, dim, pk_col in definition["foreign_keys"]:
        constraints.append(f"ALTER TABLE {schema}.{fact} ADD FOREIGN KEY ({fk_col}) REFERENCES {schema}.{dim}({pk_col})")
    pg_exec(constraints)
    print(f"Finished schema {schema}", flush=True)

spark.stop()
print("Data marts completed.", flush=True)
