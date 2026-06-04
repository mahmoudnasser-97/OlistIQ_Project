from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

spark = (SparkSession.builder.appName("olist-data-quality")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

S = "s3a://silver/"
print("\n🚀 Running Data Quality Checks...")
all_passed = True

def check(df, name, condition, desc):
    if df.filter(~condition).count() > 0:
        print(f"⚠️ [FAIL] {name}: {desc}", flush=True)
        return False
    print(f"✅ [PASS] {name}: {desc}", flush=True)
    return True

# 1. Product dimensions must not be null (our logic replaces with -1.00)
products = spark.read.format("delta").load(f"{S}silver_products")
all_passed &= check(products, "silver_products", F.col("product_weight_g").isNotNull(), "No nulls in weight")

# 2. Ratios should be strictly between 0 and 1 (or null if no sales)
sellers = spark.read.format("delta").load(f"{S}silver_sellers")
all_passed &= check(sellers, "silver_sellers", F.col("late_ratio").isNull() | F.col("late_ratio").between(0, 1), "Ratios are bounds-safe")

# 3. Valid Boolean logic
payments = spark.read.format("delta").load(f"{S}silver_payments")
all_passed &= check(payments, "silver_payments", F.col("is_installment_payment").isin(0, 1), "Installment flag is binary")

if not all_passed:
    print("\n❌ DQ Failed. Halting pipeline.", flush=True)
    sys.exit(1)

print("\n🎉 Pipeline Cleared for Gold Aggregation.", flush=True)
spark.stop()