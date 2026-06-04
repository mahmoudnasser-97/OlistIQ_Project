from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from delta import configure_spark_with_delta_pip

builder = (SparkSession.builder.appName("olist-gold-marts")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

S = "s3a://silver/"
G = "s3a://gold/"

orders = spark.read.format("delta").load(f"{S}silver_orders")
items = spark.read.format("delta").load(f"{S}silver_order_items")
products = spark.read.format("delta").load(f"{S}silver_products")
reviews = spark.read.format("delta").load(f"{S}silver_reviews")
customers = spark.read.format("delta").load(f"{S}silver_customers")
sellers = spark.read.format("delta").load(f"{S}silver_sellers")

# ── Q1: Category Performance ──────────────────────────────────────────
mart_category_performance = items.join(products, "product_id", "left") \
    .join(orders, "order_id", "left") \
    .groupBy("product_category_name") \
    .agg(
        F.sum("price").cast(DecimalType(12,2)).alias("total_revenue"),
        F.count("order_item_id").alias("total_units_sold"),
        F.avg("days_diff_estimated").cast(DecimalType(10,2)).alias("avg_days_diff_estimated"),
        F.avg("on_time_flag").cast(DecimalType(5,4)).alias("on_time_delivery_rate")
    )
mart_category_performance.write.format("delta").mode("overwrite").save(f"{G}mart_category_performance")

# ── Q2: Seller Profile (leveraging new Silver features) ───────────────
seller_review_agg = items.join(reviews, "order_id", "inner") \
    .groupBy("seller_id") \
    .agg(F.avg("review_score").cast(DecimalType(3,2)).alias("avg_review_score"))

mart_seller_profile = sellers.join(seller_review_agg, "seller_id", "left").select(
    "seller_id", "total_unique_orders", "late_ratio", "has_sales", "avg_review_score"
)
mart_seller_profile.write.format("delta").mode("overwrite").save(f"{G}mart_seller_profile")

# ── Q3: Regional Freight vs Satisfaction ──────────────────────────────
mart_regional_freight = items.join(orders, "order_id", "inner") \
    .join(customers, "customer_id", "inner") \
    .join(reviews, "order_id", "left") \
    .groupBy("customer_state", "customer_region") \
    .agg(
        F.avg("freight_value").cast(DecimalType(10,2)).alias("avg_freight_cost"),
        F.avg("review_score").cast(DecimalType(3,2)).alias("avg_customer_satisfaction")
    )
mart_regional_freight.write.format("delta").mode("overwrite").save(f"{G}mart_regional_freight")

# ── Q4: High Risk Late Sellers (>10% Late in 90 Days) ─────────────────
max_date = orders.select(F.max("order_purchase_timestamp")).collect()[0][0]

mart_late_sellers_90d = items.join(orders, "order_id", "inner") \
    .filter(F.col("order_purchase_timestamp") >= F.date_sub(F.lit(max_date), 90)) \
    .groupBy("seller_id") \
    .agg(
        F.count("order_item_id").alias("total_orders_90d"),
        F.avg(F.when(F.col("on_time_flag") == 0, 1).otherwise(0)).cast(DecimalType(5,4)).alias("late_delivery_rate_90d")
    ).filter(F.col("late_delivery_rate_90d") > 0.10)
mart_late_sellers_90d.write.format("delta").mode("overwrite").save(f"{G}mart_late_sellers_90d")

# ── Q5: Monthly Order Funnel ──────────────────────────────────────────
mart_order_funnel = orders.withColumn("cohort_month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM")) \
    .join(reviews, "order_id", "left") \
    .groupBy("cohort_month") \
    .agg(
        F.count("order_id").alias("step_1_ordered"),
        F.count("order_approved_at").alias("step_2_approved"),
        F.count("order_delivered_carrier_date").alias("step_3_shipped"),
        F.count("order_delivered_customer_date").alias("step_4_delivered"),
        F.count("review_id").alias("step_5_reviewed")
    ).sort("cohort_month")
mart_order_funnel.write.format("delta").mode("overwrite").save(f"{G}mart_order_funnel")

# ── Q6: CLV by State ──────────────────────────────────────────────────
customer_revenue = orders.join(items, "order_id", "inner") \
    .join(customers, "customer_id", "inner") \
    .groupBy("customer_unique_id", "customer_state") \
    .agg(F.sum("price").alias("total_spent"))

mart_state_clv = customer_revenue.groupBy("customer_state") \
    .agg(F.avg("total_spent").cast(DecimalType(12,2)).alias("average_clv_per_customer"))
mart_state_clv.write.format("delta").mode("overwrite").save(f"{G}mart_state_clv")

print("\n✅ Gold Marts Computed and Saved.", flush=True)
spark.stop()