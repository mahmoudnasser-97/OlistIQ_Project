"""
aggregate_gold.py - Olist Gold Layer ETL
Compatible with the latest transform_silver.py output paths:
  s3a://silver/silver_orders
  s3a://silver/silver_order_items
  s3a://silver/silver_products
  s3a://silver/silver_customers
  s3a://silver/silver_payments
  s3a://silver/silver_sellers
  s3a://silver/silver_geolocation
  s3a://silver/silver_reviews

Important compatibility decisions:
- Does NOT expect has_sales in silver_sellers.
- Does NOT expect on_time_flag in silver_orders.
- Keeps latest Silver delivery_status_detail as-is.
- Keeps latest Silver review_label as-is: Satisfied / Neutral / Unsatisfied.
- Uses geolocation as a lookup to enrich customer/seller dimensions.
- Adds Unknown (-1) rows in dimensions and coalesces missing FK lookups to -1.
- Drops previous PostgreSQL Gold tables with CASCADE before reloading to avoid old FK constraint conflicts.
"""

from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DecimalType, StructType, StructField, StringType,
    TimestampType, LongType, DoubleType, BooleanType
)

# ============================================================
# SPARK SETUP
# ============================================================

spark = (SparkSession.builder
    .appName("olist-gold-etl-final-compatible")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres-dw:5432/olist_dw"
PG_PROPS = {"user": "olist", "password": "olist", "driver": "org.postgresql.Driver"}
SILVER = "s3a://silver/"
GOLD = "s3a://gold/"

GOLD_TABLES = [
    "fct_customer_review",
    "fct_customer_payment",
    "fct_seller_fulfillment",
    "fct_orders",
    "dim_order_status_detail",
    "dim_seller",
    "dim_product",
    "dim_customer",
    "dim_date",
]

# ============================================================
# HELPERS
# ============================================================

def reset_postgres_gold_tables():
    """Drop old Gold tables and constraints before Spark JDBC overwrite."""
    print("\nResetting PostgreSQL Gold tables if they already exist...", flush=True)
    conn = psycopg2.connect(host="postgres-dw", port=5432, dbname="olist_dw", user="olist", password="olist")
    conn.autocommit = True
    cur = conn.cursor()
    for table in GOLD_TABLES:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"   dropped/cleared {table}", flush=True)
        except Exception as e:
            print(f"   warning while dropping {table}: {str(e)[:120]}", flush=True)
    cur.close()
    conn.close()


def save_to_gold(df, table_name):
    print(f"   Saving {table_name} to MinIO Gold...", flush=True)
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"{GOLD}{table_name}"))

    print(f"   Saving {table_name} to PostgreSQL...", flush=True)
    (df.write.jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPS))
    print(f"   {table_name} saved ({df.count():,} rows)", flush=True)


def add_unknown_row(df, sk_column, unknown_values):
    """Add one Unknown row with SK = -1 to a dimension table."""
    row_values = []
    for field in df.schema.fields:
        col_name = field.name
        data_type = field.dataType

        if col_name == sk_column:
            row_values.append(-1)
        elif col_name in unknown_values:
            val = unknown_values[col_name]
            if val is None:
                row_values.append(None)
            elif isinstance(data_type, DoubleType):
                row_values.append(float(val))
            elif isinstance(data_type, DecimalType):
                row_values.append(Decimal(str(val)))
            elif isinstance(data_type, (IntegerType, LongType)):
                row_values.append(int(val))
            elif isinstance(data_type, StringType):
                row_values.append(str(val))
            elif isinstance(data_type, TimestampType):
                row_values.append(val if val else datetime.now())
            elif isinstance(data_type, BooleanType):
                row_values.append(bool(val))
            else:
                row_values.append(val)
        elif isinstance(data_type, TimestampType):
            row_values.append(datetime.now())
        elif isinstance(data_type, StringType):
            row_values.append("Unknown")
        elif isinstance(data_type, (IntegerType, LongType)):
            row_values.append(-1)
        elif isinstance(data_type, DoubleType):
            row_values.append(-1.0)
        elif isinstance(data_type, DecimalType):
            row_values.append(Decimal("-1.0"))
        elif isinstance(data_type, BooleanType):
            row_values.append(False)
        else:
            row_values.append(None)

    unknown_df = spark.createDataFrame([tuple(row_values)], schema=df.schema)
    return df.unionByName(unknown_df)


def cast_existing_timestamps(df, cols):
    """Defensive only. Silver already casts timestamps, but this keeps Gold safe."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(TimestampType()))
    return df


def date_sk_from_timestamp(col_name):
    return F.when(
        F.col(col_name).isNotNull(),
        F.date_format(F.col(col_name), "yyyyMMdd").cast(IntegerType())
    ).otherwise(F.lit(19000101).cast(IntegerType()))


# ============================================================
# START
# ============================================================

print("\n" + "=" * 60)
print("LOADING SILVER TABLES")
print("=" * 60)

reset_postgres_gold_tables()

silver_orders = spark.read.format("delta").load(f"{SILVER}silver_orders")
silver_order_items = spark.read.format("delta").load(f"{SILVER}silver_order_items")
silver_customers = spark.read.format("delta").load(f"{SILVER}silver_customers")
silver_products = spark.read.format("delta").load(f"{SILVER}silver_products")
silver_sellers = spark.read.format("delta").load(f"{SILVER}silver_sellers")
silver_payments = spark.read.format("delta").load(f"{SILVER}silver_payments")
silver_reviews = spark.read.format("delta").load(f"{SILVER}silver_reviews")
silver_geolocation = spark.read.format("delta").load(f"{SILVER}silver_geolocation")

# Defensive timestamp casting. Does not change Silver business labels/metrics.
silver_orders = cast_existing_timestamps(silver_orders, [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
])
silver_order_items = cast_existing_timestamps(silver_order_items, ["shipping_limit_date"])
silver_reviews = cast_existing_timestamps(silver_reviews, ["review_creation_date", "review_answer_timestamp"])

print("✅ All Silver tables loaded", flush=True)


# ============================================================
# BUILD DIMENSIONS
# ============================================================

print("\n" + "=" * 60)
print("BUILDING DIMENSION TABLES")
print("=" * 60)

# ------------------------------------------------------------
# 1. DIM_DATE
# ------------------------------------------------------------
print("\n📅 Building dim_date...", flush=True)

date_data = []
current = datetime(2016, 1, 1)
end_date = datetime(2025, 12, 31)
while current <= end_date:
    year = current.year
    month = current.month
    day = current.day
    next_month = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    last_day_of_month = (next_month - timedelta(days=1)).day
    date_data.append((
        int(current.strftime("%Y%m%d")),
        current.strftime("%Y-%m-%d"),
        day,
        current.strftime("%A"),
        int(current.strftime("%W")),
        month,
        current.strftime("%B"),
        (month - 1) // 3 + 1,
        year,
        1 if current.weekday() >= 5 else 0,
        1 if day == 1 else 0,
        1 if day == last_day_of_month else 0,
    ))
    current += timedelta(days=1)

# Unknown / missing date row and far-future row.
date_data.append((19000101, "1900-01-01", 1, "Monday", 1, 1, "January", 1, 1900, 0, 1, 0))
date_data.append((29991231, "2999-12-31", 31, "Wednesday", 53, 12, "December", 4, 2999, 0, 0, 1))

date_schema = StructType([
    StructField("date_sk", IntegerType(), True),
    StructField("full_date", StringType(), True),
    StructField("day_number", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("week_number", IntegerType(), True),
    StructField("month_number", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("quarter_number", IntegerType(), True),
    StructField("year_number", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("is_month_start", IntegerType(), True),
    StructField("is_month_end", IntegerType(), True),
])

dim_date = spark.createDataFrame(date_data, schema=date_schema)
save_to_gold(dim_date, "dim_date")

# ------------------------------------------------------------
# 2. DIM_CUSTOMER
# ------------------------------------------------------------
print("\n👤 Building dim_customer...", flush=True)

geo_lookup_customer = (
    silver_geolocation
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("customer_latitude"),
        F.avg("geolocation_lng").alias("customer_longitude"),
    )
    .select(
        F.col("geolocation_zip_code_prefix").alias("zip_prefix"),
        "customer_latitude",
        "customer_longitude",
    )
)

dim_customer = (
    silver_customers
    .join(geo_lookup_customer, silver_customers.customer_zip_code_prefix == geo_lookup_customer.zip_prefix, "left")
    .drop("zip_prefix")
    .select(
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state", "customer_region",
        "customer_latitude", "customer_longitude",
    )
    .dropDuplicates(["customer_id"])
    .withColumn("customer_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)

dim_customer = add_unknown_row(dim_customer, "customer_sk", {
    "customer_id": "-1",
    "customer_unique_id": "-1",
    "customer_zip_code_prefix": -1,
    "customer_city": "Unknown",
    "customer_state": "Unknown",
    "customer_region": "Unknown",
    "customer_latitude": -1.0,
    "customer_longitude": -1.0,
})
save_to_gold(dim_customer, "dim_customer")

# ------------------------------------------------------------
# 3. DIM_PRODUCT
# ------------------------------------------------------------
print("\n📦 Building dim_product...", flush=True)

product_cols = [
    "product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
    "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm",
    "product_width_cm", "product_size_cm3", "logistics_size_category", "logistics_weight_category",
]
product_cols = [c for c in product_cols if c in silver_products.columns]

dim_product = (
    silver_products
    .select(product_cols)
    .dropDuplicates(["product_id"])
    .withColumn("product_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)

dim_product = add_unknown_row(dim_product, "product_sk", {
    "product_id": "-1",
    "product_category_name": "Unknown",
    "product_name_lenght": -1,
    "product_description_lenght": -1,
    "product_photos_qty": -1,
    "product_weight_g": -1.0,
    "product_length_cm": -1.0,
    "product_height_cm": -1.0,
    "product_width_cm": -1.0,
    "product_size_cm3": -1.0,
    "logistics_size_category": "Unknown",
    "logistics_weight_category": "Unknown",
})
save_to_gold(dim_product, "dim_product")

# ------------------------------------------------------------
# 4. DIM_SELLER
# ------------------------------------------------------------
print("\n🏪 Building dim_seller...", flush=True)

geo_lookup_seller = (
    silver_geolocation
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("seller_latitude"),
        F.avg("geolocation_lng").alias("seller_longitude"),
    )
    .select(
        F.col("geolocation_zip_code_prefix").alias("zip_prefix"),
        "seller_latitude",
        "seller_longitude",
    )
)

dim_seller = (
    silver_sellers
    .join(geo_lookup_seller, silver_sellers.seller_zip_code_prefix == geo_lookup_seller.zip_prefix, "left")
    .drop("zip_prefix")
    .select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state", "seller_region",
        "total_items_sold", "total_unique_orders", "early_preparations", "on_time_preparations",
        "late_preparations", "avg_handling_days", "early_ratio", "on_time_ratio", "late_ratio",
        "seller_latitude", "seller_longitude",
    )
    .dropDuplicates(["seller_id"])
    .withColumn("seller_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)

dim_seller = add_unknown_row(dim_seller, "seller_sk", {
    "seller_id": "-1",
    "seller_zip_code_prefix": -1,
    "seller_city": "Unknown",
    "seller_state": "Unknown",
    "seller_region": "Unknown",
    "total_items_sold": 0,
    "total_unique_orders": 0,
    "early_preparations": 0,
    "on_time_preparations": 0,
    "late_preparations": 0,
    "avg_handling_days": 0.0,
    "early_ratio": 0.0,
    "on_time_ratio": 0.0,
    "late_ratio": 0.0,
    "seller_latitude": -1.0,
    "seller_longitude": -1.0,
})
save_to_gold(dim_seller, "dim_seller")

# ------------------------------------------------------------
# 5. DIM_ORDER_STATUS_DETAIL
# ------------------------------------------------------------
print("\n📊 Building dim_order_status_detail...", flush=True)

dim_order_status = (
    silver_orders
    .select("order_status", "delivery_status_detail")
    .dropDuplicates(["order_status", "delivery_status_detail"])
    .withColumn("status_sk", F.monotonically_increasing_id() + 1)
)

dim_order_status = add_unknown_row(dim_order_status, "status_sk", {
    "order_status": "Unknown",
    "delivery_status_detail": "Unknown",
})
save_to_gold(dim_order_status, "dim_order_status_detail")


# ============================================================
# RELOAD DIMENSIONS FROM POSTGRESQL
# ============================================================
print("\n" + "=" * 60)
print("RELOADING DIMENSIONS FROM POSTGRESQL")
print("=" * 60)

dim_customer_pg = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)
dim_product_pg = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)
dim_seller_pg = spark.read.jdbc(url=PG_URL, table="dim_seller", properties=PG_PROPS)
dim_status_pg = spark.read.jdbc(url=PG_URL, table="dim_order_status_detail", properties=PG_PROPS)

print("✅ Dimensions reloaded", flush=True)


# ============================================================
# BUILD FACTS
# ============================================================
print("\n" + "=" * 60)
print("BUILDING FACT TABLES")
print("=" * 60)

# ------------------------------------------------------------
# 6. FCT_ORDERS
# ------------------------------------------------------------
print("\n📋 Building fct_orders...", flush=True)

customer_lookup = dim_customer_pg.select(F.col("customer_sk"), F.col("customer_id").alias("cid"))
status_lookup = dim_status_pg.select(
    F.col("status_sk"),
    F.col("order_status").alias("os"),
    F.col("delivery_status_detail").alias("dsd"),
)

fct_orders = (
    silver_orders.alias("so")
    .join(customer_lookup, F.col("so.customer_id") == F.col("cid"), "left")
    .drop("cid")
    .join(status_lookup,
          (F.col("so.order_status") == F.col("os")) &
          (F.col("so.delivery_status_detail") == F.col("dsd")),
          "left")
    .drop("os", "dsd")
    .withColumn("purchase_date_sk_fk", date_sk_from_timestamp("order_purchase_timestamp"))
    .withColumn("estimated_delivery_date_sk_fk", date_sk_from_timestamp("order_estimated_delivery_date"))
    .withColumn("actual_delivery_date_sk_fk", date_sk_from_timestamp("order_delivered_customer_date"))
    .select(
        "order_id",
        F.coalesce(F.col("customer_sk"), F.lit(-1)).cast(LongType()).alias("customer_sk_fk"),
        F.coalesce(F.col("status_sk"), F.lit(-1)).cast(LongType()).alias("status_sk_fk"),
        "purchase_date_sk_fk",
        "estimated_delivery_date_sk_fk",
        "actual_delivery_date_sk_fk",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "handling_days",
        "shipping_days",
        "total_lead_time",
        "days_diff_estimated",
        "estimated_buffer",
        "delivery_status_detail",
        "abs_days_diff",
        "total_products_price",
        "total_freight_value",
        "total_order_cost",
        "total_items_count",
        "seller_count",
        "is_multi_seller_order",
    )
    .withColumn("order_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)
save_to_gold(fct_orders, "fct_orders")

# ------------------------------------------------------------
# 7. FCT_SELLER_FULFILLMENT
# ------------------------------------------------------------
print("\n🚚 Building fct_seller_fulfillment...", flush=True)

orders_for_fulfillment = silver_orders.select("order_id", "customer_id", "order_purchase_timestamp")
product_lookup = dim_product_pg.select(F.col("product_sk"), F.col("product_id").alias("pid"))
seller_lookup = dim_seller_pg.select(F.col("seller_sk"), F.col("seller_id").alias("sid"))
customer_lookup_f = dim_customer_pg.select(F.col("customer_sk"), F.col("customer_id").alias("cid"))

fct_fulfillment = (
    silver_order_items.alias("oi")
    .join(product_lookup, F.col("oi.product_id") == F.col("pid"), "left")
    .join(seller_lookup, F.col("oi.seller_id") == F.col("sid"), "left")
    .join(orders_for_fulfillment, "order_id", "left")
    .join(customer_lookup_f, F.col("customer_id") == F.col("cid"), "left")
    .drop("pid", "sid", "cid", "customer_id")
    .withColumn("purchase_date_sk_fk", date_sk_from_timestamp("order_purchase_timestamp"))
    .withColumn("shipping_limit_date_sk_fk", date_sk_from_timestamp("shipping_limit_date"))
    .select(
        "order_id",
        "order_item_id",
        F.coalesce(F.col("customer_sk"), F.lit(-1)).cast(LongType()).alias("customer_sk_fk"),
        F.coalesce(F.col("product_sk"), F.lit(-1)).cast(LongType()).alias("product_sk_fk"),
        F.coalesce(F.col("seller_sk"), F.lit(-1)).cast(LongType()).alias("seller_sk_fk"),
        "purchase_date_sk_fk",
        "shipping_limit_date_sk_fk",
        "shipping_limit_date",
        "price",
        "freight_value",
        "seller_handling_days",
        "abs_seller_handling",
        "seller_performance",
    )
    .withColumn("fulfillment_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)
save_to_gold(fct_fulfillment, "fct_seller_fulfillment")

# ------------------------------------------------------------
# 8. FCT_CUSTOMER_PAYMENT
# ------------------------------------------------------------
print("\n💰 Building fct_customer_payment...", flush=True)

orders_for_payment = silver_orders.select("order_id", "customer_id")
customer_lookup_p = dim_customer_pg.select(F.col("customer_sk"), F.col("customer_id").alias("cid"))

fct_payment = (
    silver_payments.alias("p")
    .join(orders_for_payment, "order_id", "left")
    .join(customer_lookup_p, F.col("customer_id") == F.col("cid"), "left")
    .drop("cid", "customer_id")
    .select(
        "order_id",
        F.coalesce(F.col("customer_sk"), F.lit(-1)).cast(LongType()).alias("customer_sk_fk"),
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "is_installment_payment",
    )
    .withColumn("payment_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)
save_to_gold(fct_payment, "fct_customer_payment")

# ------------------------------------------------------------
# 9. FCT_CUSTOMER_REVIEW
# ------------------------------------------------------------
print("\n⭐ Building fct_customer_review...", flush=True)

orders_for_review = silver_orders.select("order_id", "customer_id")
customer_lookup_r = dim_customer_pg.select(F.col("customer_sk"), F.col("customer_id").alias("cid"))

fct_review = (
    silver_reviews.alias("r")
    .join(orders_for_review, "order_id", "left")
    .join(customer_lookup_r, F.col("customer_id") == F.col("cid"), "left")
    .drop("cid", "customer_id")
    .withColumn("review_creation_date_sk_fk", date_sk_from_timestamp("review_creation_date"))
    .select(
        "review_id",
        "order_id",
        F.coalesce(F.col("customer_sk"), F.lit(-1)).cast(LongType()).alias("customer_sk_fk"),
        "review_creation_date_sk_fk",
        "review_creation_date",
        "review_answer_timestamp",
        "review_score",
        "review_label",
        "review_response_delay_days",
    )
    .withColumn("review_sk", F.monotonically_increasing_id() + 1)
    .withColumn("created_at", F.current_timestamp())
)
# Remove accidental duplicate review_id column if Spark preserves both names poorly.
fct_review = fct_review.select(
    "review_sk", "review_id", "order_id", "customer_sk_fk", "review_creation_date_sk_fk",
    "review_creation_date", "review_answer_timestamp", "review_score", "review_label",
    "review_response_delay_days", "created_at"
)
save_to_gold(fct_review, "fct_customer_review")


# ============================================================
# ADD PRIMARY AND FOREIGN KEY CONSTRAINTS
# ============================================================
print("\n" + "=" * 60)
print("ADDING PRIMARY AND FOREIGN KEY CONSTRAINTS")
print("=" * 60)

try:
    conn = psycopg2.connect(host="postgres-dw", port=5432, dbname="olist_dw", user="olist", password="olist")
    conn.autocommit = True
    cur = conn.cursor()

    print("  📌 Adding Primary Keys...", flush=True)
    pks = [
        "ALTER TABLE dim_customer ADD CONSTRAINT PK_dim_customer PRIMARY KEY (customer_sk)",
        "ALTER TABLE dim_product ADD CONSTRAINT PK_dim_product PRIMARY KEY (product_sk)",
        "ALTER TABLE dim_seller ADD CONSTRAINT PK_dim_seller PRIMARY KEY (seller_sk)",
        "ALTER TABLE dim_date ADD CONSTRAINT PK_dim_date PRIMARY KEY (date_sk)",
        "ALTER TABLE dim_order_status_detail ADD CONSTRAINT PK_dim_order_status_detail PRIMARY KEY (status_sk)",
        "ALTER TABLE fct_orders ADD CONSTRAINT PK_fct_orders PRIMARY KEY (order_sk)",
        "ALTER TABLE fct_seller_fulfillment ADD CONSTRAINT PK_fct_seller_fulfillment PRIMARY KEY (fulfillment_sk)",
        "ALTER TABLE fct_customer_payment ADD CONSTRAINT PK_fct_customer_payment PRIMARY KEY (payment_sk)",
        "ALTER TABLE fct_customer_review ADD CONSTRAINT PK_fct_customer_review PRIMARY KEY (review_sk)",
    ]
    for sql in pks:
        cur.execute(sql)
        print("    ✅ PK added", flush=True)

    print("  🔗 Adding Foreign Keys...", flush=True)
    fks = [
        "ALTER TABLE fct_orders ADD CONSTRAINT FK_fct_orders_customer FOREIGN KEY (customer_sk_fk) REFERENCES dim_customer(customer_sk)",
        "ALTER TABLE fct_orders ADD CONSTRAINT FK_fct_orders_status FOREIGN KEY (status_sk_fk) REFERENCES dim_order_status_detail(status_sk)",
        "ALTER TABLE fct_orders ADD CONSTRAINT FK_fct_orders_purchase_date FOREIGN KEY (purchase_date_sk_fk) REFERENCES dim_date(date_sk)",
        "ALTER TABLE fct_orders ADD CONSTRAINT FK_fct_orders_estimated_date FOREIGN KEY (estimated_delivery_date_sk_fk) REFERENCES dim_date(date_sk)",
        "ALTER TABLE fct_orders ADD CONSTRAINT FK_fct_orders_actual_date FOREIGN KEY (actual_delivery_date_sk_fk) REFERENCES dim_date(date_sk)",
        "ALTER TABLE fct_seller_fulfillment ADD CONSTRAINT FK_fct_sf_customer FOREIGN KEY (customer_sk_fk) REFERENCES dim_customer(customer_sk)",
        "ALTER TABLE fct_seller_fulfillment ADD CONSTRAINT FK_fct_sf_product FOREIGN KEY (product_sk_fk) REFERENCES dim_product(product_sk)",
        "ALTER TABLE fct_seller_fulfillment ADD CONSTRAINT FK_fct_sf_seller FOREIGN KEY (seller_sk_fk) REFERENCES dim_seller(seller_sk)",
        "ALTER TABLE fct_seller_fulfillment ADD CONSTRAINT FK_fct_sf_purchase_date FOREIGN KEY (purchase_date_sk_fk) REFERENCES dim_date(date_sk)",
        "ALTER TABLE fct_seller_fulfillment ADD CONSTRAINT FK_fct_sf_shipping_date FOREIGN KEY (shipping_limit_date_sk_fk) REFERENCES dim_date(date_sk)",
        "ALTER TABLE fct_customer_payment ADD CONSTRAINT FK_fct_payment_customer FOREIGN KEY (customer_sk_fk) REFERENCES dim_customer(customer_sk)",
        "ALTER TABLE fct_customer_review ADD CONSTRAINT FK_fct_review_customer FOREIGN KEY (customer_sk_fk) REFERENCES dim_customer(customer_sk)",
        "ALTER TABLE fct_customer_review ADD CONSTRAINT FK_fct_review_date FOREIGN KEY (review_creation_date_sk_fk) REFERENCES dim_date(date_sk)",
    ]
    for sql in fks:
        cur.execute(sql)
        print("    ✅ FK added", flush=True)

    cur.close()
    conn.close()
    print("  ✅ All constraints added successfully!", flush=True)
except Exception as e:
    print(f"  ❌ Error adding constraints: {e}", flush=True)
    raise

print("\n" + "=" * 60)
print("🎉 GOLD LAYER COMPLETE!")
print("=" * 60)

spark.stop()
