from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
import sys

# FIXES applied vs original:
#   1. Added Delta extensions — original omitted them so reading Delta tables
#      would silently fall back to Parquet and miss Delta's schema enforcement
#   2. Added SimpleAWSCredentialsProvider — prevents credential-chain warnings
#   3. Expanded checks to cover all 8 silver tables, not just 3
#   4. Added row_count checks — an empty table passing all column checks is still wrong
#   5. sys.exit(1) on failure is kept — Dagster catches this as a non-zero exit code

builder = (
    SparkSession.builder
    .appName("olist-data-quality")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",         "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",       "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",       "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.impl",             "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.shuffle.partitions",         "4")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

S = "s3a://silver/"
print("\n🚀 Running Data Quality Checks...", flush=True)

failures = []

def check(name, condition_passed: bool, desc: str):
    """Record pass/fail. Does not stop on first failure — reports all issues."""
    if condition_passed:
        print(f"  ✅ [PASS] {name}: {desc}", flush=True)
    else:
        print(f"  ❌ [FAIL] {name}: {desc}", flush=True)
        failures.append(f"{name}: {desc}")

def check_col(df, table, condition, desc):
    """Column-level check — condition is a Spark Column expression."""
    failing = df.filter(~condition).count()
    check(table, failing == 0, desc)

def check_min_rows(df, table, min_count: int):
    """Ensure table has at least min_count rows."""
    actual = df.count()
    check(table, actual >= min_count, f"row count {actual:,} >= {min_count:,}")

# ── silver_orders ──────────────────────────────────────────────────────────
print("\n--- silver_orders ---", flush=True)
orders = spark.read.format("delta").load(f"{S}silver_orders")
check_min_rows(orders, "silver_orders", 90_000)
check_col(orders, "silver_orders", F.col("order_id").isNotNull(),                 "No null order_id (PK)")
check_col(orders, "silver_orders", F.col("customer_id").isNotNull(),              "No null customer_id (FK)")
check_col(orders, "silver_orders",
    F.col("order_status").isin(
        "delivered","shipped","canceled","invoiced",
        "processing","created","approved","unavailable","UNKNOWN"
    ),
    "order_status in allowed values"
)
check_col(orders, "silver_orders",
    F.col("total_order_cost").isNull() | (F.col("total_order_cost") >= 0),
    "total_order_cost non-negative"
)
check_col(orders, "silver_orders",
    F.col("on_time_flag").isin(0, 1),
    "on_time_flag is binary"
)

# ── silver_order_items ────────────────────────────────────────────────────
print("\n--- silver_order_items ---", flush=True)
items = spark.read.format("delta").load(f"{S}silver_order_items")
check_min_rows(items, "silver_order_items", 100_000)
check_col(items, "silver_order_items", F.col("order_id").isNotNull(),    "No null order_id")
check_col(items, "silver_order_items", F.col("product_id").isNotNull(),  "No null product_id")
check_col(items, "silver_order_items", F.col("seller_id").isNotNull(),   "No null seller_id")
check_col(items, "silver_order_items",
    F.col("price").isNull() | (F.col("price") >= 0), "price non-negative"
)
check_col(items, "silver_order_items",
    F.col("seller_performance").isin("On Time Fulfillment", "Late Fulfillment", "UNKNOWN"),
    "seller_performance in allowed values"
)

# ── silver_products ───────────────────────────────────────────────────────
print("\n--- silver_products ---", flush=True)
products = spark.read.format("delta").load(f"{S}silver_products")
check_min_rows(products, "silver_products", 30_000)
check_col(products, "silver_products", F.col("product_id").isNotNull(),           "No null product_id (PK)")
check_col(products, "silver_products", F.col("product_weight_g").isNotNull(),     "No nulls in weight (imputed to -1)")
check_col(products, "silver_products",
    F.col("logistics_size_category").isin("Small Box","Medium Box","Large Parcel"),
    "logistics_size_category in allowed values"
)

# ── silver_sellers ────────────────────────────────────────────────────────
print("\n--- silver_sellers ---", flush=True)
sellers = spark.read.format("delta").load(f"{S}silver_sellers")
check_min_rows(sellers, "silver_sellers", 3_000)
check_col(sellers, "silver_sellers", F.col("seller_id").isNotNull(),              "No null seller_id (PK)")
check_col(sellers, "silver_sellers",
    F.col("late_ratio").isNull() | F.col("late_ratio").between(0, 1),
    "late_ratio in [0,1] or null"
)
check_col(sellers, "silver_sellers",
    F.col("early_ratio").isNull() | F.col("early_ratio").between(0, 1),
    "early_ratio in [0,1] or null"
)

# ── silver_customers ──────────────────────────────────────────────────────
print("\n--- silver_customers ---", flush=True)
customers = spark.read.format("delta").load(f"{S}silver_customers")
check_min_rows(customers, "silver_customers", 90_000)
check_col(customers, "silver_customers", F.col("customer_id").isNotNull(),        "No null customer_id (PK)")
check_col(customers, "silver_customers", F.col("customer_unique_id").isNotNull(), "No null customer_unique_id")

# ── silver_reviews ────────────────────────────────────────────────────────
print("\n--- silver_reviews ---", flush=True)
reviews = spark.read.format("delta").load(f"{S}silver_reviews")
check_min_rows(reviews, "silver_reviews", 90_000)
check_col(reviews, "silver_reviews", F.col("review_id").isNotNull(),              "No null review_id (PK)")
check_col(reviews, "silver_reviews",
    F.col("review_score").isNull() | F.col("review_score").between(1, 5),
    "review_score in [1,5] or null"
)
check_col(reviews, "silver_reviews",
    F.col("review_label").isin("Positive","Neutral","Negative","UNKNOWN"),
    "review_label in allowed values"
)

# ── silver_payments ───────────────────────────────────────────────────────
print("\n--- silver_payments ---", flush=True)
payments = spark.read.format("delta").load(f"{S}silver_payments")
check_min_rows(payments, "silver_payments", 100_000)
check_col(payments, "silver_payments",
    F.col("is_installment_payment").isin(0, 1),
    "is_installment_payment is binary"
)
check_col(payments, "silver_payments",
    F.col("payment_value").isNull() | (F.col("payment_value") >= 0),
    "payment_value non-negative"
)

# ── silver_geolocation ────────────────────────────────────────────────────
print("\n--- silver_geolocation ---", flush=True)
geo = spark.read.format("delta").load(f"{S}silver_geolocation")
check_min_rows(geo, "silver_geolocation", 1_000)
check_col(geo, "silver_geolocation",
    F.col("geolocation_lat").between(-35, 6),
    "latitude in Brazil bounds [-35, 6]"
)
check_col(geo, "silver_geolocation",
    F.col("geolocation_lng").between(-75, -30),
    "longitude in Brazil bounds [-75, -30]"
)

# ── Final verdict ─────────────────────────────────────────────────────────
print("\n" + "=" * 50, flush=True)
if failures:
    print(f"❌ {len(failures)} check(s) FAILED:", flush=True)
    for f in failures:
        print(f"   • {f}", flush=True)
    spark.stop()
    sys.exit(1)   # non-zero exit → Dagster marks the step as failed
else:
    print("🎉 All checks passed. Pipeline cleared for Gold.", flush=True)

spark.stop()
