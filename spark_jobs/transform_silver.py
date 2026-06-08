
import logging
from itertools import chain

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    TimestampType,
    BooleanType,
)

BRONZE_BASE = "s3a://bronze/csv/"
SILVER_BASE = "s3a://silver/"
QA_BASE = "s3a://silver/QA_Issues/"

BRONZE_PATHS = {
    "orders": f"{BRONZE_BASE}orders/",
    "order_items": f"{BRONZE_BASE}order_items/",
    "order_payments": f"{BRONZE_BASE}order_payments/",
    "order_reviews": f"{BRONZE_BASE}order_reviews/",
    "customers": f"{BRONZE_BASE}customers/",
    "sellers": f"{BRONZE_BASE}sellers/",
    "products": f"{BRONZE_BASE}products/",
    "geolocation": f"{BRONZE_BASE}geolocation/",
    "category_translation": f"{BRONZE_BASE}category_translation/",
    "marketing_qualified_leads": f"{BRONZE_BASE}marketing_qualified_leads/",
    "closed_deals": f"{BRONZE_BASE}closed_deals/",
}

SILVER_PATHS = {
    "orders": f"{SILVER_BASE}silver_orders",
    "order_items": f"{SILVER_BASE}silver_order_items",
    "products": f"{SILVER_BASE}silver_products",
    "customers": f"{SILVER_BASE}silver_customers",
    "payments": f"{SILVER_BASE}silver_payments",
    "sellers": f"{SILVER_BASE}silver_sellers",
    "geolocation": f"{SILVER_BASE}silver_geolocation",
    "reviews": f"{SILVER_BASE}silver_reviews",
    "mql": f"{SILVER_BASE}silver_marketing_qualified_leads",
    "closed_deals": f"{SILVER_BASE}silver_closed_deals",
    "seller_acquisition_staging": f"{SILVER_BASE}seller_acquisition_staging",
    "sales_staging": f"{SILVER_BASE}sales_staging",
    "order_delivery_staging": f"{SILVER_BASE}order_delivery_staging",
    "reviews_staging": f"{SILVER_BASE}reviews_staging",
    "seller_fulfillment_staging": f"{SILVER_BASE}seller_fulfillment_staging",
    "seller_performance_monthly_staging": f"{SILVER_BASE}seller_performance_monthly_staging",
}

QA_PATHS = {
    "orders": f"{QA_BASE}silver_orders_delta",
    "order_items": f"{QA_BASE}silver_order_items_delta",
    "products": f"{QA_BASE}silver_products_delta",
    "customers": f"{QA_BASE}silver_customers_delta",
    "payments": f"{QA_BASE}silver_payments_delta",
    "sellers": f"{QA_BASE}silver_sellers_delta",
    "geolocation": f"{QA_BASE}silver_geolocation_delta",
    "reviews": f"{QA_BASE}silver_reviews_delta",
    "mql": f"{QA_BASE}silver_mql_delta",
    "closed_deals": f"{QA_BASE}silver_closed_deals_delta",
    "seller_acquisition": f"{QA_BASE}seller_acquisition_staging_delta",
    "sales_staging": f"{QA_BASE}sales_staging_delta",
    "delivery_staging": f"{QA_BASE}order_delivery_staging_delta",
    "reviews_staging": f"{QA_BASE}reviews_staging_delta",
    "seller_fulfillment_staging": f"{QA_BASE}seller_fulfillment_staging_delta",
    "seller_performance_monthly_staging": f"{QA_BASE}seller_performance_monthly_staging_delta",
}

BRAZIL_LAT_MIN = -34.0
BRAZIL_LAT_MAX = 6.0
BRAZIL_LNG_MIN = -75.0
BRAZIL_LNG_MAX = -28.0

STATE_TO_REGION = {
    "SP": "Southeast", "RJ": "Southeast", "MG": "Southeast", "ES": "Southeast",
    "PR": "South", "SC": "South", "RS": "South",
    "GO": "Central-West", "MT": "Central-West", "MS": "Central-West", "DF": "Central-West",
    "BA": "Northeast", "PE": "Northeast", "CE": "Northeast", "PB": "Northeast",
    "RN": "Northeast", "AL": "Northeast", "SE": "Northeast", "PI": "Northeast", "MA": "Northeast",
    "AM": "North", "PA": "North", "AC": "North", "RO": "North", "RR": "North", "AP": "North", "TO": "North",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("integrated_silver_pipeline")


def get_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("olist_integrated_silver_transform")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


spark = get_spark_session()


def read_bronze(name: str) -> DataFrame:
    df = spark.read.format("delta").load(BRONZE_PATHS[name])
    log.info("Loaded bronze %s with %s rows", name, df.count())
    return df


def save_silver(df: DataFrame, name: str) -> None:
    target_path = SILVER_PATHS[name]
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    log.info("Saved %s with %s rows to %s", name, df.count(), target_path)


def append_to_audit_log(new_errors: DataFrame, audit_path: str) -> None:
    if new_errors is None or new_errors.count() == 0:
        return

    for c in ["_ingested_at", "_source_file"]:
        if c in new_errors.columns:
            new_errors = new_errors.drop(c)

    if "error_reason" not in new_errors.columns:
        new_errors = new_errors.withColumn("error_reason", F.lit(None).cast("string"))
    if "error_detected_at" not in new_errors.columns:
        new_errors = new_errors.withColumn("error_detected_at", F.current_timestamp())

    try:
        existing = spark.read.format("delta").load(audit_path)
        for c in ["_ingested_at", "_source_file"]:
            if c in existing.columns:
                existing = existing.drop(c)
        combined = existing.unionByName(new_errors, allowMissingColumns=True)
    except Exception:
        combined = new_errors

    combined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(audit_path)


def clean_text(col_name: str):
    return F.lower(F.trim(F.translate(F.col(col_name).cast("string"), "", "aaaeeiooouc")))


def state_clean(col_name: str):
    return F.upper(F.trim(F.col(col_name).cast("string")))


def region_expr(state_col: str):
    mapping = F.create_map([F.lit(x) for x in chain(*STATE_TO_REGION.items())])
    return F.coalesce(mapping[F.col(state_col)], F.lit("Unknown"))


def has_col(df: DataFrame, col_name: str) -> bool:
    return col_name in df.columns


orders_raw = read_bronze("orders")
items_raw = read_bronze("order_items")
payments_raw = read_bronze("order_payments")
reviews_raw = read_bronze("order_reviews")
customers_raw = read_bronze("customers")
sellers_raw = read_bronze("sellers")
products_raw = read_bronze("products")
geo_raw = read_bronze("geolocation")
translation_raw = read_bronze("category_translation")
mql_raw = read_bronze("marketing_qualified_leads")
closed_raw = read_bronze("closed_deals")


# Geolocation
geo_casted = (
    geo_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("geolocation_zip_code_prefix", F.col("geolocation_zip_code_prefix").cast(IntegerType()))
    .withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
    .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))
    .withColumn("geolocation_city", clean_text("geolocation_city"))
    .withColumn("geolocation_state", state_clean("geolocation_state"))
    .dropDuplicates()
)

spatial_errors = (
    geo_casted
    .filter(
        (F.col("geolocation_lat") < BRAZIL_LAT_MIN) |
        (F.col("geolocation_lat") > BRAZIL_LAT_MAX) |
        (F.col("geolocation_lng") < BRAZIL_LNG_MIN) |
        (F.col("geolocation_lng") > BRAZIL_LNG_MAX) |
        F.col("geolocation_lat").isNull() |
        F.col("geolocation_lng").isNull()
    )
    .withColumn("error_reason", F.lit("Invalid or missing geolocation coordinates"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(spatial_errors, QA_PATHS["geolocation"])

geo_valid = geo_casted.filter(
    (F.col("geolocation_zip_code_prefix").isNotNull()) &
    (F.col("geolocation_lat").between(BRAZIL_LAT_MIN, BRAZIL_LAT_MAX)) &
    (F.col("geolocation_lng").between(BRAZIL_LNG_MIN, BRAZIL_LNG_MAX))
)

city_counts = geo_valid.groupBy(
    "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"
).count()

geo_rank = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("count"))

geo_mode = (
    city_counts
    .withColumn("rn", F.row_number().over(geo_rank))
    .filter(F.col("rn") == 1)
    .select(
        "geolocation_zip_code_prefix",
        F.col("geolocation_city").alias("city"),
        F.col("geolocation_state").alias("state")
    )
)

geo_median = (
    geo_valid
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.expr("percentile_approx(geolocation_lat, 0.5)").alias("median_latitude"),
        F.expr("percentile_approx(geolocation_lng, 0.5)").alias("median_longitude")
    )
)

silver_geolocation = (
    geo_median
    .join(geo_mode, "geolocation_zip_code_prefix", "left")
    .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
    .withColumn("region", region_expr("state"))
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "zip_code_prefix", "city", "state", "region",
        "median_latitude", "median_longitude",
        "source_system", "transformation_version"
    )
)
save_silver(silver_geolocation, "geolocation")


# Customers
customer_casted = (
    customers_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("customer_id", F.trim(F.col("customer_id").cast(StringType())))
    .withColumn("customer_unique_id", F.trim(F.col("customer_unique_id").cast(StringType())))
    .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast(IntegerType()))
    .withColumn("customer_city", clean_text("customer_city"))
    .withColumn("customer_state", state_clean("customer_state"))
    .dropDuplicates(["customer_id"])
)

customer_geo_errors = (
    customer_casted
    .join(silver_geolocation.select(F.col("zip_code_prefix").alias("geo_zip")), customer_casted.customer_zip_code_prefix == F.col("geo_zip"), "left")
    .filter(F.col("customer_zip_code_prefix").isNull() | F.col("geo_zip").isNull())
    .drop("geo_zip")
    .withColumn("error_reason", F.lit("Customer zip code missing or not found in geolocation"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(customer_geo_errors, QA_PATHS["customers"])

silver_customers = (
    customer_casted
    .join(
        silver_geolocation.select(
            F.col("zip_code_prefix").alias("geo_zip"),
            F.col("median_latitude"),
            F.col("median_longitude")
        ),
        customer_casted.customer_zip_code_prefix == F.col("geo_zip"),
        "left"
    )
    .drop("geo_zip")
    .withColumn("customer_region", region_expr("customer_state"))
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state", "customer_region",
        "median_latitude", "median_longitude",
        "source_system", "transformation_version"
    )
)
save_silver(silver_customers, "customers")


# Orders
orders = orders_raw.drop("_ingested_at", "_source_file")
orders = (
    orders
    .withColumn("order_id", F.trim(F.col("order_id").cast(StringType())))
    .withColumn("customer_id", F.trim(F.col("customer_id").cast(StringType())))
    .withColumn("order_status", F.lower(F.trim(F.col("order_status").cast(StringType()))))
)

for c in [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]:
    orders = orders.withColumn(c, F.col(c).cast(TimestampType()))

orphan_order_errors = (
    orders
    .join(silver_customers.select("customer_id"), "customer_id", "left_anti")
    .withColumn("error_reason", F.lit("Order customer_id not found in silver_customers"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(orphan_order_errors, QA_PATHS["orders"])

orders = (
    orders
    .withColumn("handling_days", F.datediff("order_delivered_carrier_date", "order_approved_at"))
    .withColumn("shipping_days", F.datediff("order_delivered_customer_date", "order_delivered_carrier_date"))
    .withColumn("total_lead_time", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("days_diff_estimated", F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
    .withColumn("estimated_buffer", F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
    .withColumn("delivery_status_category",
        F.when(F.col("order_status") == "canceled", "Canceled")
         .when(F.col("order_status") == "unavailable", "Unavailable")
         .when(F.col("order_status").isin("shipped", "processing", "approved", "created", "invoiced"), "In Progress")
         .when((F.col("order_status") == "delivered") & (F.col("days_diff_estimated") < 0), "Early")
         .when((F.col("order_status") == "delivered") & (F.col("days_diff_estimated") == 0), "On Time")
         .when((F.col("order_status") == "delivered") & (F.col("days_diff_estimated") > 0), "Late")
         .otherwise("Unknown")
    )
    .withColumn("delivery_status_detail",
        F.when(F.col("days_diff_estimated") <= -7, "Very Early")
         .when(F.col("days_diff_estimated") < 0, "Early")
         .when(F.col("days_diff_estimated") == 0, "On Time")
         .when(F.col("days_diff_estimated") <= 3, "Slight Delay")
         .when(F.col("days_diff_estimated") <= 7, "Late")
         .when(F.col("days_diff_estimated") > 7, "Critical Delay")
         .otherwise(F.initcap(F.col("order_status")))
    )
    .withColumn("abs_days_diff", F.abs(F.col("days_diff_estimated")))
    .withColumn("on_time_flag", F.when(F.col("days_diff_estimated") <= 0, 1).otherwise(0).cast(IntegerType()))
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .dropDuplicates(["order_id"])
)

silver_orders = orders.select(
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
    "handling_days", "shipping_days", "total_lead_time", "days_diff_estimated",
    "estimated_buffer", "delivery_status_category", "delivery_status_detail",
    "abs_days_diff", "on_time_flag", "source_system", "transformation_version"
)
save_silver(silver_orders, "orders")


# Category translation and products
translation = (
    translation_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name").cast(StringType()))))
    .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english").cast(StringType()))))
    .dropDuplicates(["product_category_name"])
)

products = (
    products_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("product_id", F.trim(F.col("product_id").cast(StringType())))
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name").cast(StringType()))))
    .withColumn("product_name_lenght", F.col("product_name_lenght").cast(IntegerType()))
    .withColumn("product_description_lenght", F.col("product_description_lenght").cast(IntegerType()))
    .withColumn("product_photos_qty", F.col("product_photos_qty").cast(IntegerType()))
    .withColumn("product_weight_g", F.col("product_weight_g").cast(DecimalType(18, 2)))
    .withColumn("product_length_cm", F.col("product_length_cm").cast(DecimalType(18, 2)))
    .withColumn("product_height_cm", F.col("product_height_cm").cast(DecimalType(18, 2)))
    .withColumn("product_width_cm", F.col("product_width_cm").cast(DecimalType(18, 2)))
    .dropDuplicates(["product_id"])
)

product_missing_errors = (
    products
    .filter(
        F.col("product_id").isNull() |
        F.col("product_weight_g").isNull() |
        F.col("product_length_cm").isNull() |
        F.col("product_height_cm").isNull() |
        F.col("product_width_cm").isNull()
    )
    .withColumn("error_reason", F.lit("Product missing key logistics fields"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(product_missing_errors, QA_PATHS["products"])

category_window = Window.partitionBy("product_category_name")
products = (
    products
    .withColumn("product_weight_g", F.coalesce(F.col("product_weight_g"), F.percentile_approx("product_weight_g", 0.5).over(category_window)).cast(DecimalType(18, 2)))
    .withColumn("product_length_cm", F.coalesce(F.col("product_length_cm"), F.percentile_approx("product_length_cm", 0.5).over(category_window)).cast(DecimalType(18, 2)))
    .withColumn("product_height_cm", F.coalesce(F.col("product_height_cm"), F.percentile_approx("product_height_cm", 0.5).over(category_window)).cast(DecimalType(18, 2)))
    .withColumn("product_width_cm", F.coalesce(F.col("product_width_cm"), F.percentile_approx("product_width_cm", 0.5).over(category_window)).cast(DecimalType(18, 2)))
    .fillna({"product_category_name": "unknown_category", "product_name_lenght": 0, "product_description_lenght": 0, "product_photos_qty": 0})
    .join(translation, "product_category_name", "left")
    .withColumn("product_category_name_english",
        F.coalesce(
            F.when(F.col("product_category_name") == "pc_gamer", F.lit("pc_gamer")),
            F.when(F.col("product_category_name") == "portateis_cozinha_e_preparadores_de_alimentos", F.lit("kitchen_portables_and_food_preparers")),
            F.col("product_category_name_english"),
            F.lit("unknown")
        )
    )
    .withColumn("product_volume_cm3", F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
    .withColumn("product_size_category",
        F.when(F.col("product_volume_cm3").isNull(), "Unknown")
         .when(F.col("product_volume_cm3") <= 5000, "Small")
         .when(F.col("product_volume_cm3") <= 20000, "Medium")
         .otherwise("Large")
    )
    .withColumn("heavy_product_flag", F.coalesce(F.col("product_weight_g") >= 10000, F.lit(False)))
    .withColumn("logistics_completeness_flag",
        F.col("product_weight_g").isNotNull() &
        F.col("product_length_cm").isNotNull() &
        F.col("product_height_cm").isNotNull() &
        F.col("product_width_cm").isNotNull()
    )
    .withColumn("catalog_completeness_flag",
        F.col("product_name_lenght").isNotNull() &
        F.col("product_description_lenght").isNotNull() &
        F.col("product_photos_qty").isNotNull()
    )
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
)

silver_products = products.select(
    "product_id", "product_category_name", "product_category_name_english",
    "product_name_lenght", "product_description_lenght", "product_photos_qty",
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    "product_volume_cm3", "product_size_category", "heavy_product_flag",
    "logistics_completeness_flag", "catalog_completeness_flag",
    "source_system", "transformation_version"
)
save_silver(silver_products, "products")


# Order items
items = (
    items_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("order_id", F.trim(F.col("order_id").cast(StringType())))
    .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
    .withColumn("product_id", F.trim(F.col("product_id").cast(StringType())))
    .withColumn("seller_id", F.trim(F.col("seller_id").cast(StringType())))
    .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast(TimestampType()))
    .withColumn("price", F.col("price").cast(DecimalType(18, 2)))
    .withColumn("freight_value", F.col("freight_value").cast(DecimalType(18, 2)))
    .dropDuplicates(["order_id", "order_item_id"])
)

order_item_orphans = (
    items.join(silver_orders.select("order_id"), "order_id", "left_anti")
    .withColumn("error_reason", F.lit("Order item order_id missing from silver_orders"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(order_item_orphans, QA_PATHS["order_items"])

items_with_order = items.join(
    silver_orders.select("order_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_status"),
    "order_id",
    "inner"
)

valid_shipping = items_with_order.filter(
    F.col("shipping_limit_date").isNotNull() &
    F.col("order_approved_at").isNotNull() &
    (F.col("shipping_limit_date") >= F.col("order_approved_at"))
)

median_shipping_row = valid_shipping.select(
    F.percentile_approx(F.datediff("shipping_limit_date", "order_approved_at"), 0.5).alias("median_days")
).collect()

median_shipping_days = int(median_shipping_row[0]["median_days"]) if median_shipping_row and median_shipping_row[0]["median_days"] is not None else 4

shipping_errors = (
    items_with_order
    .filter(
        F.col("shipping_limit_date").isNull() |
        (F.col("shipping_limit_date") < F.col("order_purchase_timestamp")) |
        (F.col("shipping_limit_date") < F.col("order_approved_at"))
    )
    .withColumn("error_reason", F.lit("Invalid shipping_limit_date, rebuilt using median approval-to-limit days"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(shipping_errors, QA_PATHS["order_items"])

silver_order_items = (
    items_with_order
    .withColumn(
        "shipping_limit_date",
        F.when(
            F.col("shipping_limit_date").isNull() |
            (F.col("shipping_limit_date") < F.col("order_purchase_timestamp")) |
            (F.col("shipping_limit_date") < F.col("order_approved_at")),
            F.coalesce(F.date_add(F.col("order_approved_at"), median_shipping_days), F.date_add(F.col("order_purchase_timestamp"), median_shipping_days)).cast(TimestampType())
        ).otherwise(F.col("shipping_limit_date"))
    )
    .withColumn("seller_handling_days", F.datediff("order_delivered_carrier_date", "shipping_limit_date"))
    .withColumn("seller_performance",
        F.when(F.col("order_status") == "canceled", "Canceled")
         .when(F.col("order_status") == "unavailable", "Unavailable")
         .when(F.col("order_status").isin("shipped", "processing", "approved", "created", "invoiced"), "In Progress")
         .when((F.col("order_status") == "delivered") & (F.col("seller_handling_days") < 0), "Early")
         .when((F.col("order_status") == "delivered") & (F.col("seller_handling_days") == 0), "On Time")
         .when((F.col("order_status") == "delivered") & (F.col("seller_handling_days") > 0), "Late Delivery to Carrier")
         .otherwise("Unknown")
    )
    .withColumn("abs_seller_handling", F.abs(F.col("seller_handling_days")))
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date",
        "price", "freight_value", "seller_handling_days", "abs_seller_handling",
        "seller_performance", "source_system", "transformation_version"
    )
)
save_silver(silver_order_items, "order_items")


# Payments
payments = (
    payments_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("order_id", F.trim(F.col("order_id").cast(StringType())))
    .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type").cast(StringType()))))
    .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
    .withColumn("payment_value", F.col("payment_value").cast(DecimalType(18, 2)))
    .dropDuplicates()
)

payment_errors = (
    payments.join(silver_orders.select("order_id"), "order_id", "left_anti")
    .withColumn("error_reason", F.lit("Payment order_id missing from silver_orders"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(payment_errors, QA_PATHS["payments"])

silver_payments = (
    payments
    .join(silver_orders.select("order_id"), "order_id", "inner")
    .withColumn("payment_installments",
        F.when((F.col("payment_type") == "credit_card") & (F.col("payment_installments") == 0), F.lit(1))
         .otherwise(F.col("payment_installments"))
    )
    .withColumn("is_installment_payment", F.when(F.col("payment_installments") > 1, 1).otherwise(0).cast(IntegerType()))
    .withColumn("payment_type_category",
        F.when(F.col("payment_type").isin("credit_card", "debit_card"), "Card")
         .when(F.col("payment_type") == "boleto", "Boleto")
         .when(F.col("payment_type") == "voucher", "Voucher")
         .otherwise("Other")
    )
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "order_id", "payment_sequential", "payment_type", "payment_type_category",
        "payment_installments", "payment_value", "is_installment_payment",
        "source_system", "transformation_version"
    )
)
save_silver(silver_payments, "payments")


# Reviews
reviews = (
    reviews_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("review_id", F.trim(F.col("review_id").cast(StringType())))
    .withColumn("order_id", F.trim(F.col("order_id").cast(StringType())))
    .withColumn("review_score", F.col("review_score").cast(IntegerType()))
    .withColumn("review_comment_title", F.trim(F.col("review_comment_title").cast(StringType())))
    .withColumn("review_comment_message", F.trim(F.col("review_comment_message").cast(StringType())))
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss"))
)

review_errors = (
    reviews
    .filter(F.col("review_id").isNull() | F.col("order_id").isNull() | F.col("review_score").isNull())
    .withColumn("error_reason", F.lit("Review critical key or score missing"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(review_errors, QA_PATHS["reviews"])

reviews = reviews.filter(F.col("review_id").isNotNull() & F.col("order_id").isNotNull() & F.col("review_score").isNotNull())

review_orphans = (
    reviews.join(silver_orders.select("order_id"), "order_id", "left_anti")
    .withColumn("error_reason", F.lit("Review order_id missing from silver_orders"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(review_orphans, QA_PATHS["reviews"])

reviews = reviews.join(silver_orders.select("order_id", "order_purchase_timestamp", "delivery_status_category", "total_lead_time", "days_diff_estimated"), "order_id", "inner")

review_window = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last())
reviews = reviews.withColumn("rn", F.row_number().over(review_window)).filter(F.col("rn") == 1).drop("rn")

review_stats = reviews.filter(
    F.col("review_creation_date").isNotNull() &
    F.col("review_answer_timestamp").isNotNull()
).select(
    F.percentile_approx(F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date"), 0.5).alias("median_response_seconds")
).collect()

median_response_seconds = review_stats[0]["median_response_seconds"] if review_stats and review_stats[0]["median_response_seconds"] is not None else 86400

silver_reviews = (
    reviews
    .withColumn("review_creation_date",
        F.when(F.col("review_creation_date").isNull(), F.date_add(F.to_date("order_purchase_timestamp"), 7).cast(TimestampType()))
         .otherwise(F.col("review_creation_date"))
    )
    .withColumn("review_answer_timestamp",
        F.when(F.col("review_answer_timestamp").isNull(), F.from_unixtime(F.unix_timestamp("review_creation_date") + median_response_seconds).cast(TimestampType()))
         .otherwise(F.col("review_answer_timestamp"))
    )
    .withColumn("review_label",
        F.when(F.col("review_score") >= 4, "Satisfied")
         .when(F.col("review_score") == 3, "Neutral")
         .otherwise("Unsatisfied")
    )
    .withColumn("sentiment_category",
        F.when(F.col("review_score") >= 4, "Positive")
         .when(F.col("review_score") == 3, "Neutral")
         .otherwise("Negative")
    )
    .withColumn("review_response_delay_days", F.datediff("review_answer_timestamp", "review_creation_date"))
    .withColumn("review_response_delay_hours", F.round((F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date")) / 3600, 2).cast(DecimalType(18, 2)))
    .withColumn("is_same_day_response", F.datediff("review_answer_timestamp", "review_creation_date") == 0)
    .withColumn("negative_review_flag", F.col("review_score") <= 2)
    .withColumn("neutral_review_flag", F.col("review_score") == 3)
    .withColumn("positive_review_flag", F.col("review_score") >= 4)
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "review_id", "order_id", "review_score", "review_creation_date",
        "review_answer_timestamp", "review_label", "sentiment_category",
        "review_response_delay_days", "review_response_delay_hours", "is_same_day_response",
        "negative_review_flag", "neutral_review_flag", "positive_review_flag",
        "source_system", "transformation_version"
    )
)
save_silver(silver_reviews, "reviews")


# Marketing qualified leads and closed deals
silver_mql = (
    mql_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("mql_id", F.trim(F.col("mql_id").cast(StringType())))
    .withColumn("first_contact_date", F.to_timestamp("first_contact_date"))
    .withColumn("landing_page_id", F.lower(F.trim(F.col("landing_page_id").cast(StringType()))))
    .withColumn("origin", F.lower(F.trim(F.col("origin").cast(StringType()))))
    .withColumn("origin", F.coalesce(F.col("origin"), F.lit("unknown")))
    .dropDuplicates(["mql_id"])
    .withColumn("source_system", F.lit("OLIST_MARKETING_FUNNEL"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select("mql_id", "first_contact_date", "landing_page_id", "origin", "source_system", "transformation_version")
)

mql_errors = (
    silver_mql
    .filter(F.col("mql_id").isNull())
    .withColumn("error_reason", F.lit("MQL missing mql_id"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(mql_errors, QA_PATHS["mql"])
silver_mql = silver_mql.filter(F.col("mql_id").isNotNull())
save_silver(silver_mql, "mql")

silver_closed = (
    closed_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("mql_id", F.trim(F.col("mql_id").cast(StringType())))
    .withColumn("seller_id", F.trim(F.col("seller_id").cast(StringType())))
    .withColumn("sdr_id", F.trim(F.col("sdr_id").cast(StringType())))
    .withColumn("sr_id", F.trim(F.col("sr_id").cast(StringType())))
    .withColumn("won_date", F.to_timestamp("won_date"))
    .withColumn("business_segment", F.lower(F.trim(F.col("business_segment").cast(StringType()))))
    .withColumn("lead_type", F.lower(F.trim(F.col("lead_type").cast(StringType()))))
    .withColumn("lead_behaviour_profile", F.lower(F.trim(F.col("lead_behaviour_profile").cast(StringType()))))
    .withColumn("business_type", F.lower(F.trim(F.col("business_type").cast(StringType()))))
    .withColumn("declared_monthly_revenue", F.col("declared_monthly_revenue").cast(DecimalType(18, 2)))
    .dropDuplicates(["mql_id", "seller_id"])
    .withColumn("source_system", F.lit("OLIST_MARKETING_FUNNEL"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "mql_id", "seller_id", "sdr_id", "sr_id", "won_date",
        "business_segment", "lead_type", "lead_behaviour_profile",
        "business_type", "declared_monthly_revenue",
        "source_system", "transformation_version"
    )
)

closed_errors = (
    silver_closed
    .filter(F.col("mql_id").isNull() | F.col("seller_id").isNull())
    .withColumn("error_reason", F.lit("Closed deal missing mql_id or seller_id"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(closed_errors, QA_PATHS["closed_deals"])
silver_closed = silver_closed.filter(F.col("mql_id").isNotNull() & F.col("seller_id").isNotNull())
save_silver(silver_closed, "closed_deals")

mql_for_acquisition = silver_mql.select(
    "mql_id",
    "first_contact_date",
    "landing_page_id",
    F.col("origin").alias("marketing_origin")
)

closed_for_acquisition = silver_closed.select(
    "mql_id",
    "seller_id",
    "won_date",
    "business_segment",
    "lead_type",
    "lead_behaviour_profile",
    "business_type",
    "declared_monthly_revenue"
)

seller_acquisition_staging = (
    mql_for_acquisition
    .join(closed_for_acquisition, "mql_id", "left")
    .withColumn("acquisition_source", F.coalesce(F.col("marketing_origin"), F.lit("unknown")))
    .withColumn("converted_flag", F.col("seller_id").isNotNull())
    .withColumn(
        "days_to_convert",
        F.when(
            F.col("won_date").isNotNull(),
            F.datediff(F.to_date("won_date"), F.to_date("first_contact_date"))
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    .withColumn(
        "seller_acquisition_segment",
        F.when(F.col("converted_flag") == False, "Not Converted")
         .when(F.col("days_to_convert") <= 7, "Fast Conversion")
         .when(F.col("days_to_convert") <= 30, "Normal Conversion")
         .when(F.col("days_to_convert").isNotNull(), "Slow Conversion")
         .otherwise("Unknown Conversion")
    )
    .withColumn("source_system", F.lit("OLIST_MARKETING_FUNNEL"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "mql_id", "seller_id", "first_contact_date", "landing_page_id",
        "marketing_origin", "acquisition_source", "won_date",
        "business_segment", "lead_type", "lead_behaviour_profile",
        "business_type", "declared_monthly_revenue", "days_to_convert",
        "converted_flag", "seller_acquisition_segment",
        "source_system", "transformation_version"
    )
)
save_silver(seller_acquisition_staging, "seller_acquisition_staging")


# Sellers enriched with seller performance and marketing
seller_perf = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        F.count("order_item_id").alias("total_items_sold"),
        F.countDistinct("order_id").alias("total_unique_orders"),
        F.sum(F.when(F.col("seller_performance") == "Early", 1).otherwise(0)).alias("early_preparations"),
        F.sum(F.when(F.col("seller_performance") == "On Time", 1).otherwise(0)).alias("on_time_preparations"),
        F.sum(F.when(F.col("seller_performance").like("Late%"), 1).otherwise(0)).alias("late_preparations"),
        F.round(F.avg("seller_handling_days"), 2).alias("avg_handling_days"),
    )
    .withColumn("early_ratio", F.round((F.col("early_preparations") / F.col("total_items_sold")) * 100, 2))
    .withColumn("on_time_ratio", F.round((F.col("on_time_preparations") / F.col("total_items_sold")) * 100, 2))
    .withColumn("late_ratio", F.round((F.col("late_preparations") / F.col("total_items_sold")) * 100, 2))
)

seller_acq_one = (
    seller_acquisition_staging
    .filter(F.col("seller_id").isNotNull())
    .withColumn("rn", F.row_number().over(Window.partitionBy("seller_id").orderBy(F.col("won_date").asc_nulls_last())))
    .filter(F.col("rn") == 1)
    .select(
        "seller_id", "marketing_origin", "acquisition_source", "business_segment",
        "lead_type", "lead_behaviour_profile", "days_to_convert",
        "converted_flag", "seller_acquisition_segment"
    )
)

sellers_base = (
    sellers_raw
    .drop("_ingested_at", "_source_file")
    .withColumn("seller_id", F.trim(F.col("seller_id").cast(StringType())))
    .withColumn("seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast(IntegerType()))
    .withColumn("seller_city", clean_text("seller_city"))
    .withColumn("seller_state", state_clean("seller_state"))
    .dropDuplicates(["seller_id"])
)

seller_geo_errors = (
    sellers_base
    .join(silver_geolocation.select(F.col("zip_code_prefix").alias("geo_zip")), sellers_base.seller_zip_code_prefix == F.col("geo_zip"), "left")
    .filter(F.col("seller_zip_code_prefix").isNull() | F.col("geo_zip").isNull())
    .drop("geo_zip")
    .withColumn("error_reason", F.lit("Seller zip code missing or not found in geolocation"))
    .withColumn("error_detected_at", F.current_timestamp())
)
append_to_audit_log(seller_geo_errors, QA_PATHS["sellers"])

silver_sellers = (
    sellers_base
    .join(
        silver_geolocation.select(
            F.col("zip_code_prefix").alias("geo_zip"),
            F.col("median_latitude"),
            F.col("median_longitude")
        ),
        sellers_base.seller_zip_code_prefix == F.col("geo_zip"),
        "left"
    )
    .drop("geo_zip")
    .withColumn("seller_region", region_expr("seller_state"))
    .join(seller_perf, "seller_id", "left")
    .join(seller_acq_one, "seller_id", "left")
    .fillna({
        "total_items_sold": 0,
        "total_unique_orders": 0,
        "early_preparations": 0,
        "on_time_preparations": 0,
        "late_preparations": 0,
        "avg_handling_days": 0.0,
        "early_ratio": 0.0,
        "on_time_ratio": 0.0,
        "late_ratio": 0.0,
        "marketing_origin": "Not Tracked",
        "acquisition_source": "Not Tracked",
        "business_segment": "Not Tracked",
        "lead_type": "Not Tracked",
        "lead_behaviour_profile": "Not Tracked",
        "seller_acquisition_segment": "Not Tracked",
    })
    .withColumn("has_sales", F.col("total_items_sold") > 0)
    .withColumn("converted_flag", F.coalesce(F.col("converted_flag"), F.lit(False)))
    .withColumn("days_to_convert", F.coalesce(F.col("days_to_convert"), F.lit(-1)))
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
    .select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
        "seller_region", "median_latitude", "median_longitude",
        "total_items_sold", "total_unique_orders", "early_preparations",
        "on_time_preparations", "late_preparations", "avg_handling_days",
        "early_ratio", "on_time_ratio", "late_ratio", "has_sales",
        "marketing_origin", "acquisition_source", "business_segment",
        "lead_type", "lead_behaviour_profile", "days_to_convert",
        "converted_flag", "seller_acquisition_segment",
        "source_system", "transformation_version"
    )
)
save_silver(silver_sellers, "sellers")


# Sales staging
payments_agg = (
    silver_payments
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.max("payment_installments").alias("total_payment_installments"),
        F.first("payment_type", ignorenulls=True).alias("payment_type"),
        F.first("payment_type_category", ignorenulls=True).alias("payment_type_category")
    )
    .withColumn("installment_flag", F.col("total_payment_installments") > 1)
)

order_totals = (
    silver_order_items
    .groupBy("order_id")
    .agg(
        F.sum(F.col("price") + F.col("freight_value")).alias("total_order_item_value"),
        F.count("order_item_id").alias("total_items_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.first("seller_id", ignorenulls=True).alias("primary_seller_id"),
        F.sum("freight_value").alias("freight_total_value")
    )
    .withColumn("is_multi_seller_order", F.col("seller_count") > 1)
)

sales_staging = (
    silver_order_items
    .join(silver_orders.select("order_id", "customer_id", "order_purchase_timestamp"), "order_id", "left")
    .join(payments_agg, "order_id", "left")
    .join(order_totals.select("order_id", "total_order_item_value"), "order_id", "left")
    .join(silver_products.select("product_id", "product_volume_cm3"), "product_id", "left")
    .withColumn("gross_item_value", F.col("price") + F.col("freight_value"))
    .withColumn("item_sales_ratio", F.when(F.col("total_order_item_value") > 0, F.col("gross_item_value") / F.col("total_order_item_value")).otherwise(F.lit(0.0)))
    .withColumn("allocated_payment_value", F.col("item_sales_ratio") * F.col("total_payment_value"))
    .withColumn("freight_ratio", F.when(F.col("gross_item_value") > 0, F.col("freight_value") / F.col("gross_item_value")).otherwise(F.lit(0.0)))
    .withColumn("seller_item_count_in_order", F.count("*").over(Window.partitionBy("order_id", "seller_id")))
    .withColumn("high_ticket_order_flag", F.col("allocated_payment_value") >= 500)
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
)
save_silver(sales_staging, "sales_staging")


# Delivery staging
delivery_staging = (
    silver_orders
    .join(order_totals, "order_id", "left")
    .join(silver_customers.select("customer_id", "customer_region", F.col("median_latitude").alias("customer_lat"), F.col("median_longitude").alias("customer_lng")), "customer_id", "left")
    .join(
        silver_sellers.select(
            F.col("seller_id").alias("primary_seller_id"),
            F.col("seller_region"),
            F.col("seller_state"),
            F.col("median_latitude").alias("seller_lat"),
            F.col("median_longitude").alias("seller_lng")
        ),
        "primary_seller_id",
        "left"
    )
    .withColumn("distance_bucket",
        F.when(F.col("seller_region").isNull() | F.col("customer_region").isNull(), "Unknown")
         .when(F.col("seller_region") == F.col("customer_region"), "Same Region")
         .otherwise("Cross Region")
    )
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
)
save_silver(delivery_staging, "order_delivery_staging")


# Reviews staging
reviews_staging = (
    silver_reviews
    .join(
        delivery_staging.select(
            "order_id", "primary_seller_id", "order_status", "total_lead_time", "days_diff_estimated",
            "delivery_status_category", "distance_bucket", "is_multi_seller_order"
        ),
        "order_id",
        "left"
    )
    .withColumnRenamed("total_lead_time", "delivery_duration_days")
    .withColumnRenamed("days_diff_estimated", "delay_days")
    .withColumn("delayed_delivery_review_flag", (F.col("delay_days") > 0) & (F.col("review_score") <= 3))
    .withColumn("delivery_experience_segment",
        F.when((F.col("delay_days") <= 0) & (F.col("review_score") >= 4), "Good Experience")
         .when((F.col("delay_days") > 0) & (F.col("review_score") <= 3), "Delivery Impacted Review")
         .otherwise("Mixed Experience")
    )
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
)
save_silver(reviews_staging, "reviews_staging")


# Seller fulfillment staging
fulfillment_base = (
    silver_order_items
    .join(silver_orders.select("order_id", "order_purchase_timestamp"), "order_id", "left")
    .join(silver_products.select("product_id", "product_volume_cm3"), "product_id", "left")
    .join(silver_sellers.select("seller_id", "acquisition_source"), "seller_id", "left")
    .withColumn("performance_year", F.year("order_purchase_timestamp"))
    .withColumn("performance_month", F.month("order_purchase_timestamp"))
)

seller_monthly_orders = (
    fulfillment_base
    .groupBy("seller_id", "performance_year", "performance_month")
    .agg(F.countDistinct("order_id").alias("seller_monthly_orders"))
)

seller_fulfillment_staging = (
    fulfillment_base
    .join(seller_monthly_orders, ["seller_id", "performance_year", "performance_month"], "left")
    .drop("performance_year", "performance_month")
    .withColumn("freight_ratio", F.when((F.col("price") + F.col("freight_value")) > 0, F.col("freight_value") / (F.col("price") + F.col("freight_value"))).otherwise(F.lit(0.0)))
    .withColumn("seller_item_count_in_order", F.count("*").over(Window.partitionBy("order_id", "seller_id")))
    .withColumn("workload_bucket",
        F.when(F.col("seller_monthly_orders") >= 100, "Overloaded")
         .when(F.col("seller_monthly_orders") >= 50, "High Volume")
         .when(F.col("seller_monthly_orders") >= 10, "Normal Volume")
         .otherwise("Low Volume")
    )
    .withColumn("is_overloaded_seller", F.col("workload_bucket") == "Overloaded")
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
)
save_silver(seller_fulfillment_staging, "seller_fulfillment_staging")


# Seller performance monthly staging
delivery_single = delivery_staging.filter(F.coalesce(F.col("is_multi_seller_order"), F.lit(False)) == False)
reviews_single = reviews_staging.filter(F.coalesce(F.col("is_multi_seller_order"), F.lit(False)) == False)

delivery_monthly = (
    delivery_single
    .withColumn("performance_year", F.year("order_purchase_timestamp"))
    .withColumn("performance_month", F.month("order_purchase_timestamp"))
    .groupBy("primary_seller_id", "performance_year", "performance_month")
    .agg(
        F.count("order_id").alias("monthly_orders"),
        F.avg("total_lead_time").alias("avg_shipping_days"),
        F.avg(F.when(F.col("days_diff_estimated") <= 0, 1).otherwise(0)).alias("on_time_rate"),
        F.avg("days_diff_estimated").alias("avg_delay_days"),
        F.sum(F.when(F.col("days_diff_estimated") > 0, 1).otherwise(0)).alias("delayed_orders_count")
    )
)

reviews_monthly = (
    reviews_single
    .withColumn("performance_year", F.year("review_creation_date"))
    .withColumn("performance_month", F.month("review_creation_date"))
    .groupBy("primary_seller_id", "performance_year", "performance_month")
    .agg(
        F.avg("review_score").alias("avg_review_score"),
        F.avg(F.when(F.col("negative_review_flag") == True, 1).otherwise(0)).alias("negative_review_rate"),
        F.count("review_id").alias("monthly_review_count")
    )
)

fulfillment_monthly = (
    seller_fulfillment_staging
    .withColumn("performance_year", F.year("order_purchase_timestamp"))
    .withColumn("performance_month", F.month("order_purchase_timestamp"))
    .groupBy("seller_id", "performance_year", "performance_month")
    .agg(
        F.avg("seller_monthly_orders").alias("avg_monthly_workload"),
        F.avg("freight_ratio").alias("avg_freight_ratio"),
        F.avg("product_volume_cm3").alias("avg_product_volume_cm3")
    )
)

seller_performance_monthly_staging = (
    delivery_monthly
    .join(
        reviews_monthly,
        ["primary_seller_id", "performance_year", "performance_month"],
        "left"
    )
    .join(
        fulfillment_monthly,
        (F.col("primary_seller_id") == F.col("seller_id")) &
        (delivery_monthly.performance_year == fulfillment_monthly.performance_year) &
        (delivery_monthly.performance_month == fulfillment_monthly.performance_month),
        "left"
    )
    .drop(fulfillment_monthly.performance_year)
    .drop(fulfillment_monthly.performance_month)
    .drop("seller_id")
    .withColumnRenamed("primary_seller_id", "seller_id")
    .fillna({"avg_review_score": 0.0, "negative_review_rate": 0.0, "monthly_review_count": 0})
)

seller_window = Window.partitionBy("seller_id").orderBy("performance_year", "performance_month")

seller_performance_monthly_staging = (
    seller_performance_monthly_staging
    .withColumn("previous_month_orders", F.lag("monthly_orders").over(seller_window))
    .withColumn("is_new_seller_month", F.col("previous_month_orders").isNull())
    .withColumn("volume_growth_rate",
        F.when(F.col("previous_month_orders").isNull() | (F.col("previous_month_orders") == 0), F.lit(None).cast(DoubleType()))
         .otherwise((F.col("monthly_orders") - F.col("previous_month_orders")) / F.col("previous_month_orders"))
    )
    .withColumn("seller_growth_category",
        F.when(F.col("is_new_seller_month") == True, "New Seller")
         .when(F.col("volume_growth_rate") >= 0.50, "High Growth")
         .when(F.col("volume_growth_rate") >= 0.10, "Moderate Growth")
         .when(F.col("volume_growth_rate") <= -0.10, "Declining")
         .otherwise("Stable")
    )
    .withColumn("seller_performance_category",
        F.when(F.col("on_time_rate") >= 0.90, "Excellent")
         .when(F.col("on_time_rate") >= 0.75, "Good")
         .when(F.col("on_time_rate") >= 0.50, "Fair")
         .otherwise("Poor")
    )
    .withColumn("source_system", F.lit("OLIST"))
    .withColumn("transformation_version", F.lit("1.0"))
)

save_silver(seller_performance_monthly_staging, "seller_performance_monthly_staging")

spark.stop()
print("Integrated silver transformation completed.", flush=True)
