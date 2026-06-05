import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    avg, count, sum as spark_sum,
    max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
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
    StructField("order_estimated_delivery_date", StringType())
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
# This function is called for every micro-batch Spark processes

def write_to_redis(batch_df, batch_id):
    """
    Called by Spark for every micro-batch
    """
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        return

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Convert to Pandas for easy row-by-row processing
    pdf = batch_df.toPandas()

    print(f"[Batch {batch_id}] Processing {len(pdf)} events")

    for _, row in pdf.iterrows():
        # 1. Storing each raw event so Streamlit can show a live feed
        event_key = f"event:{row['order_id']}"
        event_data = {
            "order_id":         row["order_id"],
            "order_status":     row["order_status"],
            "customer_state":   row["customer_state"],
            "product_category": row["product_category_name"],
            "payment_type":     row["payment_type"],
            "payment_value":    str(row["payment_value"]),
            "review_score":     str(row["review_score"]),
            "price":            str(row["price"]),
            "freight_value":    str(row["freight_value"]),
            "event_timestamp":  row["event_timestamp"]
        }
        r.hset(event_key, mapping=event_data)
        r.expire(event_key, 86400)  # expire after 24 hours

        # 2. Pushing order_id to a list so Streamlit knows what's new
        # We keep only the latest 200 events in this list.
        r.lpush("recent_events", row["order_id"])
        r.ltrim("recent_events", 0, 199)

        # 3. Incrementing running counters per order status
        r.incr(f"counters:status:{row['order_status']}")

        # 4. Incrementing running counters per payment type
        r.incr(f"counters:payment:{row['payment_type']}")

        # 5. Increment running counters per product category
        r.incr(f"counters:category:{row['product_category_name']}")

        # ----------------------------------------------------------
        # 6. Incrementing running counters per customer state
        # ----------------------------------------------------------
        r.incr(f"counters:state:{row['customer_state']}")

        # 7. Accumulate total revenue (we store as float in a key)
        r.incrbyfloat("metrics:total_revenue", float(row["payment_value"]))
        r.incrbyfloat("metrics:total_freight", float(row["freight_value"]))

        # 8. Track review scores for running average calculation
        # We store sum and count separately so we can compute
        # the average at read time: avg = sum / count
        r.incrbyfloat("metrics:review_score_sum", float(row["review_score"]))
        r.incr("metrics:review_score_count")

        # 9. Increment total order counter
        r.incr("metrics:total_orders")

    print(f"[Batch {batch_id}] Written to Redis successfully")


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

    print("Spark session created")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")

    # READ FROM KAFKA
 
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # PARSE JSON
    
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    )

    # FLATTEN THE NESTED STRUCTURE

    flat_stream = parsed_stream.select(
        col("data.event_timestamp"),
        col("data.order.order_id"),
        col("data.order.order_status"),
        col("data.order.order_purchase_timestamp"),
        col("data.customer.customer_state"),
        col("data.customer.customer_city"),
        col("data.product.product_category_name"),
        col("data.product.product_weight_g"),
        col("data.order_item.price"),
        col("data.order_item.freight_value"),
        col("data.payment.payment_type"),
        col("data.payment.payment_value"),
        col("data.payment.payment_installments"),
        col("data.review.review_score"),
        col("data.seller.seller_state")
    )

    # WRITE TO REDIS USING FOREACH BATCH

    query = (
        flat_stream.writeStream
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming query started. Processing every 10 seconds")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming()