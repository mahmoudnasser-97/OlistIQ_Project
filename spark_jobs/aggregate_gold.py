from datetime import datetime, timedelta
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, DateType
from delta import configure_spark_with_delta_pip

SILVER = "s3a://silver/"
GOLD = "s3a://gold/"
PG_URL = "jdbc:postgresql://postgres-dw:5432/olist_dw"
PG_PROPS = {"user": "olist", "password": "olist", "driver": "org.postgresql.Driver"}

FINAL_GOLD_TABLES = [
    "dim_customer",
    "dim_seller",
    "dim_delivery_status",
    "dim_geolocation",
    "dim_date",
    "dim_product",
    "dim_review_sentiment",
    "dim_payment_type",
    "fct_order_sales",
    "fct_customer_reviews",
    "fct_order_delivery",
    "fct_seller_fulfillment",
]

OLD_PUBLIC_TABLES = [
    "seller_acquisition_effectiveness_mart",
    "seller_performance_mart",
    "delivery_performance_mart",
    "customer_satisfaction_mart",
    "sales_mart",
    "dim_seller_enriched",
    "fct_marketing_funnel",
    "dim_business_segment",
    "dim_marketing_channel",
    "fct_customer_payment",
    "fct_customer_review",
    "fct_orders",
    "dim_order_status_detail",
]

MART_SCHEMAS = [
    "sales_mart",
    "delivery_performance_mart",
    "customer_satisfaction_mart",
    "seller_performance_mart",
    "seller_acquisition_effectiveness_mart",
]

builder = (
    SparkSession.builder
    .appName("olist_gold_aggregate")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
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

def pg_exec(statements):
    conn = psycopg2.connect(host="postgres-dw", port=5432, dbname="olist_dw", user="olist", password="olist")
    conn.autocommit = True
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    cur.close()
    conn.close()

def reset_postgres():
    statements = []
    for schema in MART_SCHEMAS:
        statements.append(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    for table in OLD_PUBLIC_TABLES + FINAL_GOLD_TABLES:
        statements.append(f"DROP TABLE IF EXISTS public.{table} CASCADE")
    pg_exec(statements)

def read_silver(name):
    return spark.read.format("delta").load(f"{SILVER}{name}")

def save_gold(df, table):
    df.cache()
    row_count = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}{table}")
    df.write.jdbc(url=PG_URL, table=f"public.{table}", mode="overwrite", properties=PG_PROPS)
    df.unpersist()
    print(f"Saved {table}. Rows: {row_count}", flush=True)

def date_sk(ts_col):
    return F.when(F.col(ts_col).isNotNull(), F.date_format(F.col(ts_col), "yyyyMMdd").cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))

reset_postgres()

silver_customers = read_silver("silver_customers")
silver_sellers = read_silver("silver_sellers")
silver_products = read_silver("silver_products")
silver_geo = read_silver("silver_geolocation")
sales_staging = read_silver("sales_staging")
delivery_staging = read_silver("order_delivery_staging")
reviews_staging = read_silver("reviews_staging")
fulfillment_staging = read_silver("seller_fulfillment_staging")

date_rows = []
current = datetime(2015, 1, 1)
end = datetime(2020, 12, 31)
while current <= end:
    date_rows.append((
        int(current.strftime("%Y%m%d")),
        current.date(),
        current.year,
        ((current.month - 1) // 3) + 1,
        current.month,
        f"{current.month:02d}",
        current.strftime("%B"),
        current.strftime("%Y-%m"),
        int(current.strftime("%U")),
        current.day,
        int(current.strftime("%j")),
        current.strftime("%A"),
        "Weekend" if current.weekday() >= 5 else "Business Day",
        current.weekday() >= 5,
        current.weekday() < 5,
        current.day == 1,
        (current + timedelta(days=1)).month != current.month,
        current.month in [1, 4, 7, 10] and current.day == 1,
        current.month in [3, 6, 9, 12] and ((current + timedelta(days=1)).month != current.month),
        current.month == 1 and current.day == 1,
        current.month == 12 and current.day == 31,
    ))
    current += timedelta(days=1)

dim_date = spark.createDataFrame(date_rows, [
    "date_sk", "full_date", "year", "quarter", "month", "month_key", "month_name", "year_month",
    "week_of_year", "day", "day_of_year", "day_name", "day_type", "is_weekend", "is_business_day",
    "is_month_start", "is_month_end", "is_quarter_start", "is_quarter_end", "is_year_start", "is_year_end"
]).withColumn("gold_loaded_at", F.current_timestamp()).withColumn("source_system", F.lit("OLIST")).withColumn("transformation_version", F.lit("1.0"))

dim_geolocation = (
    silver_geo
    .withColumn("geolocation_sk", F.row_number().over(Window.orderBy("zip_code_prefix", "city", "state")))
    .select("geolocation_sk", "zip_code_prefix", "city", "state", "region", "median_latitude", "median_longitude", "source_system", "transformation_version")
    .withColumn("gold_loaded_at", F.current_timestamp())
)

dim_customer = (
    silver_customers
    .withColumn("customer_location_type",
        F.when(F.col("customer_city").isin("sao paulo", "rio de janeiro", "brasilia", "salvador", "fortaleza", "belo horizonte", "curitiba", "manaus", "recife", "porto alegre"), "Metropolitan")
         .when(F.col("customer_state").isin("SP", "RJ", "MG"), "Urban")
         .otherwise("Regional"))
    .withColumn("customer_sk", F.row_number().over(Window.orderBy("customer_id")))
    .select("customer_sk", "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
            "customer_region", "customer_location_type", "median_latitude", "median_longitude", "source_system", "transformation_version")
    .withColumn("gold_loaded_at", F.current_timestamp())
)

dim_seller = (
    silver_sellers
    .withColumn("seller_location_type",
        F.when(F.col("seller_city").isin("sao paulo", "rio de janeiro", "brasilia", "salvador", "fortaleza", "belo horizonte", "curitiba", "manaus", "recife", "porto alegre"), "Metropolitan")
         .when(F.col("seller_state").isin("SP", "RJ", "MG"), "Urban")
         .otherwise("Regional"))
    .withColumn("effective_start_date", F.current_date())
    .withColumn("effective_end_date", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True))
    .withColumn("seller_sk", F.row_number().over(Window.orderBy("seller_id")))
    .select("seller_sk", "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state", "seller_region",
            "median_latitude", "median_longitude", "marketing_origin", "acquisition_source", "business_segment",
            "lead_type", "lead_behaviour_profile", "days_to_convert", "converted_flag", "seller_acquisition_segment",
            "seller_location_type", "effective_start_date", "effective_end_date", "is_current", "source_system", "transformation_version")
    .withColumn("gold_loaded_at", F.current_timestamp())
)

dim_product = (
    silver_products
    .withColumn("product_sk", F.row_number().over(Window.orderBy("product_id")))
    .select("product_sk", "product_id", "product_category_name", "product_category_name_english", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm",
            "product_width_cm", "product_volume_cm3", "product_size_category", "heavy_product_flag",
            "logistics_completeness_flag", "catalog_completeness_flag", "source_system", "transformation_version")
    .withColumn("gold_loaded_at", F.current_timestamp())
)

dim_delivery_status = (
    delivery_staging.select("delivery_status_category", "order_status").dropDuplicates()
    .withColumn("delivery_status_sk", F.row_number().over(Window.orderBy("delivery_status_category", "order_status")))
    .withColumn("delivery_status_group",
        F.when(F.col("delivery_status_category") == "On Time", "Successful")
         .when(F.col("delivery_status_category").isin("Slight Delay", "Late", "Critical Delay"), "Delayed")
         .otherwise("Other"))
    .select("delivery_status_sk", "order_status", "delivery_status_category", "delivery_status_group")
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .withColumn("gold_loaded_at", F.current_timestamp())
)

dim_review_sentiment = (
    reviews_staging.select("sentiment_category").dropDuplicates()
    .withColumn("review_sentiment_sk", F.row_number().over(Window.orderBy("sentiment_category")))
    .withColumn("sentiment_score_band",
        F.when(F.col("sentiment_category") == "Positive", "Score 4-5")
         .when(F.col("sentiment_category") == "Neutral", "Score 3")
         .when(F.col("sentiment_category") == "Negative", "Score 1-2")
         .otherwise("Unknown"))
    .select("review_sentiment_sk", "sentiment_category", "sentiment_score_band")
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .withColumn("gold_loaded_at", F.current_timestamp())
)

dim_payment_type = (
    sales_staging.select("payment_type", "payment_type_category", "installment_flag").dropDuplicates()
    .withColumn("payment_type_sk", F.row_number().over(Window.orderBy("payment_type", "installment_flag")))
    .select("payment_type_sk", "payment_type", "payment_type_category", "installment_flag")
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .withColumn("gold_loaded_at", F.current_timestamp())
)

for name, df in [
    ("dim_date", dim_date), ("dim_geolocation", dim_geolocation), ("dim_customer", dim_customer),
    ("dim_seller", dim_seller), ("dim_product", dim_product), ("dim_delivery_status", dim_delivery_status),
    ("dim_review_sentiment", dim_review_sentiment), ("dim_payment_type", dim_payment_type)
]:
    save_gold(df, name)

dim_customer_pg = spark.read.jdbc(url=PG_URL, table="public.dim_customer", properties=PG_PROPS)
dim_seller_pg = spark.read.jdbc(url=PG_URL, table="public.dim_seller", properties=PG_PROPS)
dim_product_pg = spark.read.jdbc(url=PG_URL, table="public.dim_product", properties=PG_PROPS)
dim_delivery_status_pg = spark.read.jdbc(url=PG_URL, table="public.dim_delivery_status", properties=PG_PROPS)
dim_review_sentiment_pg = spark.read.jdbc(url=PG_URL, table="public.dim_review_sentiment", properties=PG_PROPS)
dim_payment_type_pg = spark.read.jdbc(url=PG_URL, table="public.dim_payment_type", properties=PG_PROPS)

fct_order_sales = (
    sales_staging
    .join(dim_product_pg.select("product_id", "product_sk"), "product_id", "left")
    .join(dim_seller_pg.select("seller_id", "seller_sk"), "seller_id", "left")
    .join(dim_customer_pg.select("customer_id", "customer_sk"), "customer_id", "left")
    .join(dim_payment_type_pg.select("payment_type", "installment_flag", "payment_type_sk"), ["payment_type", "installment_flag"], "left")
    .withColumn("sales_date_sk", date_sk("order_purchase_timestamp"))
    .withColumn("high_freight_item_flag", F.col("freight_ratio") > 0.30)
    .withColumn("sales_fact_sk", F.row_number().over(Window.orderBy("order_id", "order_item_id")))
    .select(
        "sales_fact_sk", "order_id", "order_item_id",
        F.col("product_sk").alias("product_sk_fk"), F.col("seller_sk").alias("seller_sk_fk"),
        F.col("customer_sk").alias("customer_sk_fk"), F.col("payment_type_sk").alias("payment_type_sk_fk"),
        "sales_date_sk", "price", "freight_value", "gross_item_value", "total_order_item_value",
        "allocated_payment_value", "item_sales_ratio", "freight_ratio", "product_volume_cm3",
        "seller_item_count_in_order", "total_payment_installments", "installment_flag",
        "high_ticket_order_flag", "high_freight_item_flag", "source_system", "transformation_version"
    )
    .withColumn("gold_loaded_at", F.current_timestamp())
)

fct_order_delivery = (
    delivery_staging
    .join(dim_customer_pg.select("customer_id", "customer_sk"), "customer_id", "left")
    .join(dim_seller_pg.select(F.col("seller_id").alias("primary_seller_id"), "seller_sk"), "primary_seller_id", "left")
    .join(dim_delivery_status_pg.select("order_status", "delivery_status_category", "delivery_status_sk"), ["order_status", "delivery_status_category"], "left")
    .withColumn("purchase_date_sk", date_sk("order_purchase_timestamp"))
    .withColumn("estimated_delivery_date_sk", date_sk("order_estimated_delivery_date"))
    .withColumn("actual_delivery_date_sk", date_sk("order_delivered_customer_date"))
    .withColumn("on_time_delivery_flag", F.col("days_diff_estimated") <= 0)
    .withColumn("late_delivery_flag", F.col("days_diff_estimated") > 0)
    .withColumn("delivery_fact_sk", F.row_number().over(Window.orderBy("order_id")))
    .select(
        "delivery_fact_sk", "order_id", F.col("customer_sk").alias("customer_sk_fk"),
        F.col("seller_sk").alias("seller_sk_fk"), F.col("delivery_status_sk").alias("delivery_status_sk_fk"),
        "purchase_date_sk", "estimated_delivery_date_sk", "actual_delivery_date_sk",
        "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date", F.col("total_lead_time").alias("delivery_duration_days"),
        F.col("days_diff_estimated").alias("delay_days"), F.col("estimated_buffer").alias("buffer_days"), "freight_total_value", "total_items_count", "seller_count",
        "is_multi_seller_order", "distance_bucket", "seller_region", "customer_region",
        "on_time_delivery_flag", "late_delivery_flag", "source_system", "transformation_version"
    )
    .withColumn("gold_loaded_at", F.current_timestamp())
)

fct_customer_reviews = (
    reviews_staging
    .join(dim_seller_pg.select(F.col("seller_id").alias("primary_seller_id"), "seller_sk"), "primary_seller_id", "left")
    .join(dim_review_sentiment_pg.select("sentiment_category", "review_sentiment_sk"), "sentiment_category", "left")
    .withColumn("review_date_sk", date_sk("review_creation_date"))
    .withColumn("response_date_sk", date_sk("review_answer_timestamp"))
    .withColumn("low_rating_flag", F.col("review_score") <= 2)
    .withColumn("excellent_rating_flag", F.col("review_score") == 5)
    .withColumn("review_fact_sk", F.row_number().over(Window.orderBy("review_id")))
    .select(
        "review_fact_sk", "review_id", "order_id", F.col("seller_sk").alias("seller_sk_fk"),
        F.col("review_sentiment_sk").alias("review_sentiment_sk_fk"), "review_date_sk", "response_date_sk",
        "review_score", F.col("review_response_delay_days").alias("review_response_days"), "delivery_duration_days", "delay_days",
        "negative_review_flag", "neutral_review_flag", "positive_review_flag",
        "delayed_delivery_review_flag", "low_rating_flag", "excellent_rating_flag",
        "is_multi_seller_order", "sentiment_category", "delivery_experience_segment",
        "delivery_status_category", "distance_bucket", "source_system", "transformation_version"
    )
    .withColumn("gold_loaded_at", F.current_timestamp())
)

fct_seller_fulfillment = (
    fulfillment_staging
    .join(dim_seller_pg.select("seller_id", "seller_sk"), "seller_id", "left")
    .join(dim_product_pg.select("product_id", "product_sk"), "product_id", "left")
    .withColumn("purchase_date_sk", date_sk("order_purchase_timestamp"))
    .withColumn("high_freight_item_flag", F.col("freight_ratio") >= 0.30)
    .withColumn("high_workload_flag", F.col("workload_bucket").isin("High Volume", "Overloaded"))
    .withColumn("fulfillment_fact_sk", F.row_number().over(Window.orderBy("order_id", "order_item_id")))
    .select(
        "fulfillment_fact_sk", "order_id", "order_item_id",
        F.col("seller_sk").alias("seller_sk_fk"), F.col("product_sk").alias("product_sk_fk"),
        "purchase_date_sk", "price", "freight_value", "freight_ratio", "product_volume_cm3",
        "seller_item_count_in_order", "seller_monthly_orders", "workload_bucket",
        "is_overloaded_seller", "high_workload_flag", "high_freight_item_flag",
        "acquisition_source", "source_system", "transformation_version"
    )
    .withColumn("gold_loaded_at", F.current_timestamp())
)

for name, df in [
    ("fct_order_sales", fct_order_sales), ("fct_order_delivery", fct_order_delivery),
    ("fct_customer_reviews", fct_customer_reviews), ("fct_seller_fulfillment", fct_seller_fulfillment)
]:
    save_gold(df, name)

constraints = [
    "ALTER TABLE public.dim_customer ADD PRIMARY KEY (customer_sk)",
    "ALTER TABLE public.dim_seller ADD PRIMARY KEY (seller_sk)",
    "ALTER TABLE public.dim_product ADD PRIMARY KEY (product_sk)",
    "ALTER TABLE public.dim_date ADD PRIMARY KEY (date_sk)",
    "ALTER TABLE public.dim_delivery_status ADD PRIMARY KEY (delivery_status_sk)",
    "ALTER TABLE public.dim_review_sentiment ADD PRIMARY KEY (review_sentiment_sk)",
    "ALTER TABLE public.dim_payment_type ADD PRIMARY KEY (payment_type_sk)",
    "ALTER TABLE public.dim_geolocation ADD PRIMARY KEY (geolocation_sk)",
    "ALTER TABLE public.fct_order_sales ADD PRIMARY KEY (sales_fact_sk)",
    "ALTER TABLE public.fct_order_delivery ADD PRIMARY KEY (delivery_fact_sk)",
    "ALTER TABLE public.fct_customer_reviews ADD PRIMARY KEY (review_fact_sk)",
    "ALTER TABLE public.fct_seller_fulfillment ADD PRIMARY KEY (fulfillment_fact_sk)",
    "ALTER TABLE public.fct_order_sales ADD FOREIGN KEY (product_sk_fk) REFERENCES public.dim_product(product_sk)",
    "ALTER TABLE public.fct_order_sales ADD FOREIGN KEY (seller_sk_fk) REFERENCES public.dim_seller(seller_sk)",
    "ALTER TABLE public.fct_order_sales ADD FOREIGN KEY (customer_sk_fk) REFERENCES public.dim_customer(customer_sk)",
    "ALTER TABLE public.fct_order_sales ADD FOREIGN KEY (payment_type_sk_fk) REFERENCES public.dim_payment_type(payment_type_sk)",
    "ALTER TABLE public.fct_order_sales ADD FOREIGN KEY (sales_date_sk) REFERENCES public.dim_date(date_sk)",
    "ALTER TABLE public.fct_order_delivery ADD FOREIGN KEY (customer_sk_fk) REFERENCES public.dim_customer(customer_sk)",
    "ALTER TABLE public.fct_order_delivery ADD FOREIGN KEY (seller_sk_fk) REFERENCES public.dim_seller(seller_sk)",
    "ALTER TABLE public.fct_order_delivery ADD FOREIGN KEY (delivery_status_sk_fk) REFERENCES public.dim_delivery_status(delivery_status_sk)",
    "ALTER TABLE public.fct_order_delivery ADD FOREIGN KEY (purchase_date_sk) REFERENCES public.dim_date(date_sk)",
    "ALTER TABLE public.fct_customer_reviews ADD FOREIGN KEY (seller_sk_fk) REFERENCES public.dim_seller(seller_sk)",
    "ALTER TABLE public.fct_customer_reviews ADD FOREIGN KEY (review_sentiment_sk_fk) REFERENCES public.dim_review_sentiment(review_sentiment_sk)",
    "ALTER TABLE public.fct_customer_reviews ADD FOREIGN KEY (review_date_sk) REFERENCES public.dim_date(date_sk)",
    "ALTER TABLE public.fct_seller_fulfillment ADD FOREIGN KEY (seller_sk_fk) REFERENCES public.dim_seller(seller_sk)",
    "ALTER TABLE public.fct_seller_fulfillment ADD FOREIGN KEY (product_sk_fk) REFERENCES public.dim_product(product_sk)",
    "ALTER TABLE public.fct_seller_fulfillment ADD FOREIGN KEY (purchase_date_sk) REFERENCES public.dim_date(date_sk)",
]
pg_exec(constraints)

spark.stop()
print("Gold layer completed.", flush=True)
