import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)

# CONFIGURATION

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "olist_orders_stream"
REDIS_HOST = "redis"
REDIS_PORT = 6379
CHECKPOINT_LOCATION = "/tmp/spark_checkpoints"

# SCHEMA DEFINITION

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("order_approved_at", StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType()),
    StructField("delivery_days", IntegerType())
])

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType())
])

seller_schema = StructType([
    StructField("seller_id", StringType()),
    StructField("seller_zip_code_prefix", StringType()),
    StructField("seller_city", StringType()),
    StructField("seller_state", StringType())
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_category_name_english", StringType()),
    StructField("product_name_length", IntegerType()),
    StructField("product_description_length", IntegerType()),
    StructField("product_photos_qty", IntegerType()),
    StructField("product_weight_g", IntegerType()),
    StructField("product_length_cm", IntegerType()),
    StructField("product_height_cm", IntegerType()),
    StructField("product_width_cm", IntegerType())
])

order_item_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", StringType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType())
])

payment_schema = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value", DoubleType())
])

review_schema = StructType([
    StructField("review_id", StringType()),
    StructField("order_id", StringType()),
    StructField("review_score", IntegerType()),
    StructField("review_comment_title", StringType()),
    StructField("review_comment_message", StringType()),
    StructField("review_has_comment", BooleanType()),
    StructField("review_creation_date", StringType()),
    StructField("review_answer_timestamp", StringType())
])

event_schema = StructType([
    StructField("event_timestamp", StringType()),
    StructField("order", order_schema),
    StructField("customer", customer_schema),
    StructField("seller", seller_schema),
    StructField("product", product_schema),
    StructField("order_item", order_item_schema),
    StructField("payment", payment_schema),
    StructField("review", review_schema)
])

# REDIS WRITER

def write_to_redis(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        return

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    pdf = batch_df.toPandas()

    print(f"[Batch {batch_id}] Processing {len(pdf)} events...")

    for _, row in pdf.iterrows():

        # 1. Store raw event hash with expanded fields
        event_key = f"event:{row['order_id']}"
        event_data = {
            "order_id":                     row["order_id"],
            "order_status":                 row["order_status"],
            "customer_state":               row["customer_state"],
            "customer_city":                row["customer_city"],
            "seller_state":                 row["seller_state"],
            "seller_city":                  row["seller_city"],
            "product_category":             row["product_category_name_english"],
            "product_category_pt":          row["product_category_name"],
            "product_photos_qty":           str(row["product_photos_qty"]),
            "product_weight_g":             str(row["product_weight_g"]),
            "payment_type":                 row["payment_type"],
            "payment_value":                str(row["payment_value"]),
            "payment_installments":         str(row["payment_installments"]),
            "price":                        str(row["price"]),
            "freight_value":                str(row["freight_value"]),
            "review_score":                 str(row["review_score"]),
            "review_has_comment":           str(row["review_has_comment"]),
            "delivery_days":                str(row["delivery_days"]),
            "event_timestamp":              row["event_timestamp"],
            "order_purchase_timestamp":     row["order_purchase_timestamp"]
        }
        r.hset(event_key, mapping=event_data)
        r.expire(event_key, 86400)

        # 2. Recent events list — same pattern as before
        r.lpush("recent_events", row["order_id"])
        r.ltrim("recent_events", 0, 199)

        # 3. Order status counters — same pattern as before
        r.incr(f"counters:status:{row['order_status']}")

        # 4. Payment type counters — same pattern as before
        r.incr(f"counters:payment:{row['payment_type']}")

        # 5. Product category counters — NOW using English name
        r.incr(f"counters:category:{row['product_category_name_english']}")

        # 6. Customer state counters — same pattern as before
        r.incr(f"counters:state:{row['customer_state']}")

        # 7. Seller state counters — NEW
        r.incr(f"counters:seller_state:{row['seller_state']}")

        # 8. Payment installments counters
        r.incr(f"counters:installments:{row['payment_installments']}")

        # 9. Product photos qty counters — NEW
        # ----------------------------------------------------------
        r.incr(f"counters:photos:{row['product_photos_qty']}")

        # 10. Review score counters — NEW (for score distribution bar)
        r.incr(f"counters:review_score:{row['review_score']}")

        # 11. Comment vs no comment counter — NEW
        if row["review_has_comment"]:
            r.incr("counters:review_has_comment:yes")
        else:
            r.incr("counters:review_has_comment:no")

        # 12. Revenue metrics — same pattern as before
        r.incrbyfloat("metrics:total_revenue", float(row["payment_value"]))
        r.incrbyfloat("metrics:total_freight", float(row["freight_value"]))

        # 13. Review score running average — same pattern as before
        r.incrbyfloat("metrics:review_score_sum", float(row["review_score"]))
        r.incr("metrics:review_score_count")

        # 14. Total orders counter — same pattern as before
        r.incr("metrics:total_orders")

        # 15. Delivery days accumulator
        r.incrbyfloat("metrics:delivery_days_sum", float(row["delivery_days"]))
        r.incr("metrics:delivery_days_count")

        # 16. Weight accumulator
        r.incrbyfloat("metrics:weight_sum", float(row["product_weight_g"]))
        r.incr("metrics:weight_count")

        # 17. Freight ratio accumulator
        if float(row["payment_value"]) > 0:
            freight_ratio = float(row["freight_value"]) / float(row["payment_value"])
            r.incrbyfloat("metrics:freight_ratio_sum", freight_ratio)
            r.incr("metrics:freight_ratio_count")

        # 18. Price bucket counters
        price = float(row["price"])
        if price < 50:
            bucket = "Under R$50"
        elif price < 100:
            bucket = "R$50-100"
        elif price < 200:
            bucket = "R$100-200"
        elif price < 400:
            bucket = "R$200-400"
        else:
            bucket = "Above R$400"
        r.incr(f"counters:price_bucket:{bucket}")

        # 19. Freight bucket counters
        freight = float(row["freight_value"])
        if freight < 15:
            f_bucket = "Under R$15"
        elif freight < 30:
            f_bucket = "R$15-30"
        elif freight < 50:
            f_bucket = "R$30-50"
        else:
            f_bucket = "Above R$50"
        r.incr(f"counters:freight_bucket:{f_bucket}")

        # 20. Customer city counters
        r.incr(f"counters:customer_city:{row['customer_city']}")

        # 21. Seller city counters
        r.incr(f"counters:seller_city:{row['seller_city']}")

    print(f"[Batch {batch_id}] Written to Redis successfully.")


# SPARK SESSION

def create_spark_session():
    jars = ",".join([
        "/opt/spark_jobs/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
        "/opt/spark_jobs/jars/kafka-clients-3.4.0.jar",
        "/opt/spark_jobs/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
        "/opt/spark_jobs/jars/commons-pool2-2.11.1.jar"
    ])

    return (
        SparkSession.builder
        .appName("OlistStreamingPipeline")
        .config("spark.jars", jars)
        .getOrCreate()
    )


# MAIN STREAMING PIPELINE

def run_streaming():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created.")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    )

    # EXPANDED FLAT STREAM
    flat_stream = parsed_stream.select(
        # Top-level
        col("data.event_timestamp"),

        # Order fields
        col("data.order.order_id"),
        col("data.order.order_status"),
        col("data.order.order_purchase_timestamp"),
        col("data.order.delivery_days"),

        # Customer fields
        col("data.customer.customer_state"),
        col("data.customer.customer_city"),

        # Seller fields
        col("data.seller.seller_state"),
        col("data.seller.seller_city"),

        # Product fields
        col("data.product.product_category_name"),
        col("data.product.product_category_name_english"),
        col("data.product.product_photos_qty"),
        col("data.product.product_weight_g"),
        col("data.product.product_description_length"),

        # Order item fields
        col("data.order_item.price"),
        col("data.order_item.freight_value"),

        # Payment fields
        col("data.payment.payment_type"),
        col("data.payment.payment_value"),
        col("data.payment.payment_installments"),

        # Review fields
        col("data.review.review_score"),
        col("data.review.review_has_comment")
    )

    query = (
        flat_stream.writeStream
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming query started. Processing every 10 seconds.")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming()