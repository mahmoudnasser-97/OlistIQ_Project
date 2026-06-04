from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, DoubleType, BooleanType, StringType
from delta import configure_spark_with_delta_pip

# ── Spark Setup ────────────────────────────────────────────────────────────
builder = (SparkSession.builder
    .appName("olist-silver-transform-final")
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

B = "s3a://bronze/csv/"
S = "s3a://silver/"

# ── Helper Functions ───────────────────────────────────────────────────────
def get_region(state_col):
    return (
        F.when(state_col.isin('SP','RJ','MG','ES'), 'Southeast')
         .when(state_col.isin('PR','RS','SC'), 'South')
         .when(state_col.isin('BA','PE','CE','RN','MA','PB','AL','SE','PI'), 'Northeast')
         .when(state_col.isin('MT','MS','GO','DF'), 'Central-West')
         .otherwise('North')
    )

def impute_strings(df):
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit("UNKNOWN")))
    return df

# Load All Bronze Tables
raw_orders = spark.read.format("delta").load(f"{B}orders")
raw_items = spark.read.format("delta").load(f"{B}order_items")
raw_products = spark.read.format("delta").load(f"{B}products")
raw_reviews = spark.read.format("delta").load(f"{B}order_reviews")
raw_customers = spark.read.format("delta").load(f"{B}customers")
raw_sellers = spark.read.format("delta").load(f"{B}sellers")
raw_payments = spark.read.format("delta").load(f"{B}order_payments")
raw_geo = spark.read.format("delta").load(f"{B}geolocation")
translation = spark.read.format("delta").load(f"{B}category_translation")

# 🛠️ 1. PROCESS: silver_order_items
print("Processing: silver_order_items...", flush=True)
items_enriched = raw_items.join(
    F.broadcast(raw_orders.select("order_id", "order_purchase_timestamp", "order_delivered_carrier_date")), 
    "order_id", "left"
)

silver_order_items = (
    items_enriched
    .withColumn("price", F.col("price").cast(DecimalType(10,2)))
    .withColumn("freight_value", F.col("freight_value").cast(DecimalType(10,2)))
    .withColumn("seller_handling_days", F.datediff(F.col("order_delivered_carrier_date"), F.col("order_purchase_timestamp")).cast(IntegerType()))
    .withColumn("abs_seller_handling", F.abs(F.col("seller_handling_days")))
    .withColumn("seller_performance", F.when(F.col("order_delivered_carrier_date") <= F.col("shipping_limit_date"), "On Time Fulfillment").otherwise("Late Fulfillment"))
    .drop("order_purchase_timestamp", "order_delivered_carrier_date")
)
silver_order_items = impute_strings(silver_order_items).dropDuplicates(["order_id", "order_item_id"])
silver_order_items.cache()
silver_order_items.write.format("delta").mode("overwrite").save(f"{S}silver_order_items")

# 🛠️ 2. PROCESS: silver_orders
print("Processing: silver_orders...", flush=True)
items_agg = silver_order_items.groupBy("order_id").agg(
    F.sum("price").cast(DecimalType(10,2)).alias("total_products_price"),
    F.sum("freight_value").cast(DecimalType(10,2)).alias("total_freight_value"),
    F.count("order_item_id").cast(IntegerType()).alias("total_items_count"),
    F.countDistinct("seller_id").cast(IntegerType()).alias("seller_count")
)

silver_orders = (
    raw_orders.join(items_agg, "order_id", "left")
    .withColumn("handling_days", F.datediff(F.col("order_delivered_carrier_date"), F.col("order_purchase_timestamp")).cast(IntegerType()))
    .withColumn("shipping_days", F.datediff(F.col("order_delivered_customer_date"), F.col("order_delivered_carrier_date")).cast(IntegerType()))
    .withColumn("total_lead_time", F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")).cast(IntegerType()))
    .withColumn("days_diff_estimated", F.datediff(F.col("order_estimated_delivery_date"), F.col("order_delivered_customer_date")).cast(IntegerType()))
    .withColumn("estimated_buffer", F.datediff(F.col("order_estimated_delivery_date"), F.col("order_purchase_timestamp")).cast(IntegerType()))
    .withColumn("delivery_status_detail", F.when(F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), "Delivered on time")
                                           .when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), "Delayed")
                                           .otherwise("Pending"))
    .withColumn("abs_days_diff", F.abs(F.col("days_diff_estimated")))
    .withColumn("total_order_cost", (F.coalesce(F.col("total_products_price"), F.lit(0)) + F.coalesce(F.col("total_freight_value"), F.lit(0))).cast(DecimalType(10,2)))
    .withColumn("on_time_flag", F.when(F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), 1).otherwise(0).cast(IntegerType()))
    .withColumn("is_multi_seller_order", F.when(F.col("seller_count") > 1, 1).otherwise(0).cast(IntegerType()))
)
silver_orders = impute_strings(silver_orders).dropDuplicates(["order_id"])
silver_orders.write.format("delta").mode("overwrite").save(f"{S}silver_orders")

# 🛠️ 3. PROCESS: silver_products

print("Processing: silver_products...", flush=True)

# Define exactly what columns we need from the lookup dataframe
clean_translation = translation.select("product_category_name", "product_category_name_english")

silver_products = (
    # Join using our filtered dataframe to eliminate metadata duplication entirely
    raw_products.join(clean_translation, "product_category_name", "left")
    .withColumn("product_category_name", F.coalesce(F.col("product_category_name_english"), F.col("product_category_name"), F.lit("Unknown Category")))
    .withColumn("product_weight_g", F.coalesce(F.col("product_weight_g").cast(DecimalType(10,2)), F.lit(-1.00).cast(DecimalType(10,2))))
    .withColumn("product_length_cm", F.coalesce(F.col("product_length_cm").cast(DecimalType(10,2)), F.lit(-1.00).cast(DecimalType(10,2))))
    .withColumn("product_height_cm", F.coalesce(F.col("product_height_cm").cast(DecimalType(10,2)), F.lit(-1.00).cast(DecimalType(10,2))))
    .withColumn("product_width_cm", F.coalesce(F.col("product_width_cm").cast(DecimalType(10,2)), F.lit(-1.00).cast(DecimalType(10,2))))
    .withColumn("product_size_cm3", (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).cast(DecimalType(10,2)))
    .withColumn("logistics_size_category", F.when(F.col("product_size_cm3") <= 5000, "Small Box")
                                            .when(F.col("product_size_cm3") <= 20000, "Medium Box")
                                            .otherwise("Large Parcel"))
    .withColumn("logistics_weight_category", F.when(F.col("product_weight_g") <= 2000, "Lightweight")
                                              .when(F.col("product_weight_g") <= 10000, "Midweight")
                                              .otherwise("Heavyweight"))
    .drop("product_category_name_english")
)
silver_products = impute_strings(silver_products).dropDuplicates(["product_id"])
silver_products.write.format("delta").mode("overwrite").save(f"{S}silver_products")

# 🛠️ 4. PROCESS: silver_reviews
print("Processing: silver_reviews...", flush=True)
silver_reviews = (
    raw_reviews
    .withColumn("review_score", F.col("review_score").cast(IntegerType()))
    .withColumn("review_label", F.when(F.col("review_score") >= 4, "Positive")
                                 .when(F.col("review_score") == 3, "Neutral")
                                 .otherwise("Negative"))
    .withColumn("review_response_delay_days", F.datediff(F.col("review_answer_timestamp"), F.col("review_creation_date")).cast(IntegerType()))
)
silver_reviews = impute_strings(silver_reviews).dropDuplicates(["review_id"])
silver_reviews.write.format("delta").mode("overwrite").save(f"{S}silver_reviews")

# 🛠️ 5. PROCESS: silver_customers
print("Processing: silver_customers...", flush=True)
silver_customers = (
    raw_customers
    .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast(IntegerType()))
    .withColumn("customer_region", get_region(F.col("customer_state")))
)
silver_customers = impute_strings(silver_customers).dropDuplicates(["customer_id"])
silver_customers.write.format("delta").mode("overwrite").save(f"{S}silver_customers")

# 🛠️ 6. PROCESS: silver_sellers
print("Processing: silver_sellers...", flush=True)
# Join items and orders to get the dates needed for seller metrics
seller_base = raw_items.join(raw_orders.select("order_id", "order_delivered_carrier_date", "order_purchase_timestamp"), "order_id", "inner")

seller_metrics = seller_base.groupBy("seller_id").agg(
    F.count("order_item_id").cast(LongType()).alias("total_items_sold"),
    F.countDistinct("order_id").cast(LongType()).alias("total_unique_orders"),
    F.sum(F.when(F.col("order_delivered_carrier_date") < F.col("shipping_limit_date"), 1).otherwise(0)).cast(LongType()).alias("early_preparations"),
    F.sum(F.when(F.col("order_delivered_carrier_date") == F.col("shipping_limit_date"), 1).otherwise(0)).cast(LongType()).alias("on_time_preparations"),
    F.sum(F.when(F.col("order_delivered_carrier_date") > F.col("shipping_limit_date"), 1).otherwise(0)).cast(LongType()).alias("late_preparations"),
    F.avg(F.datediff(F.col("order_delivered_carrier_date"), F.col("order_purchase_timestamp"))).cast(DoubleType()).alias("avg_handling_days")
)

silver_sellers = (
    raw_sellers.join(seller_metrics, "seller_id", "left")
    .withColumn("seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast(IntegerType()))
    .withColumn("early_ratio", (F.col("early_preparations") / F.when(F.col("total_items_sold") == 0, None).otherwise(F.col("total_items_sold"))).cast(DoubleType()))
    .withColumn("on_time_ratio", (F.col("on_time_preparations") / F.when(F.col("total_items_sold") == 0, None).otherwise(F.col("total_items_sold"))).cast(DoubleType()))
    .withColumn("late_ratio", (F.col("late_preparations") / F.when(F.col("total_items_sold") == 0, None).otherwise(F.col("total_items_sold"))).cast(DoubleType()))
    .withColumn("has_sales", F.when(F.col("total_items_sold") > 0, True).otherwise(False).cast(BooleanType()))
    .withColumn("seller_region", get_region(F.col("seller_state")))
)
silver_sellers = impute_strings(silver_sellers).dropDuplicates(["seller_id"])
silver_sellers.write.format("delta").mode("overwrite").save(f"{S}silver_sellers")

# 🛠️ 7. PROCESS: silver_payments
print("Processing: silver_payments...", flush=True)
silver_payments = (
    raw_payments
    .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
    .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
    .withColumn("payment_value", F.col("payment_value").cast(DecimalType(10,2)))
    .withColumn("is_installment_payment", F.when(F.col("payment_installments") > 1, 1).otherwise(0).cast(IntegerType()))
)
silver_payments = impute_strings(silver_payments).dropDuplicates(["order_id", "payment_sequential"])
silver_payments.write.format("delta").mode("overwrite").save(f"{S}silver_payments")

# 🛠️ 8. PROCESS: silver_geolocation
print("Processing: silver_geolocation...", flush=True)
silver_geolocation = (
    raw_geo
    .withColumn("geolocation_zip_code_prefix", F.col("geolocation_zip_code_prefix").cast(IntegerType()))
    .withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
    .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))
)
# Geolocation often has multiple entries per zip code, drop dupes based on prefix to create a clean dimension lookup
silver_geolocation = impute_strings(silver_geolocation).dropDuplicates(["geolocation_zip_code_prefix"])
silver_geolocation.write.format("delta").mode("overwrite").save(f"{S}silver_geolocation")

print("\n✅ All Silver Tables Generated Perfectly.", flush=True)
spark.stop()
