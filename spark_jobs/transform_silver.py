"""
================================================================================
  UNIFIED SILVER REFINING PIPELINE (FULLY CORRECTED - with metadata column fix)
  ==============================================================================
  Tables: silver_orders, silver_order_items, silver_products, silver_customers,
          silver_payments, silver_sellers, silver_geolocation, silver_reviews
  ==============================================================================

  FIX in this version:
    - append_to_audit_log now drops _ingested_at and _source_file metadata columns
    - Prevents DELTA_DUPLICATE_COLUMNS_FOUND errors during QA audit writes
================================================================================
"""

import logging
from itertools import chain
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, DecimalType, TimestampType, BooleanType
)

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("UnifiedSilverPipeline")

# =============================================================================
# PATH CONFIGURATION — Bronze Delta
# =============================================================================
BRONZE_BASE = "s3a://bronze/csv/"

BRONZE_PATHS = {
    "orders":              f"{BRONZE_BASE}orders/",
    "order_items":         f"{BRONZE_BASE}order_items/",
    "order_payments":      f"{BRONZE_BASE}order_payments/",
    "order_reviews":       f"{BRONZE_BASE}order_reviews/",
    "customers":           f"{BRONZE_BASE}customers/",
    "sellers":             f"{BRONZE_BASE}sellers/",
    "products":            f"{BRONZE_BASE}products/",
    "geolocation":         f"{BRONZE_BASE}geolocation/",
    "category_translation": f"{BRONZE_BASE}category_translation/",
}

# =============================================================================
# PATH CONFIGURATION — Silver Delta
# =============================================================================
SILVER_BASE = "s3a://silver/"
QA_BASE     = "s3a://silver/QA_Issues/"

SILVER_PATHS = {
    # IMPORTANT: Keep these exact folder names for orchestration and Gold compatibility
    "orders":              f"{SILVER_BASE}silver_orders",
    "order_items":         f"{SILVER_BASE}silver_order_items",
    "products":            f"{SILVER_BASE}silver_products",
    "customers":           f"{SILVER_BASE}silver_customers",
    "payments":            f"{SILVER_BASE}silver_payments",
    "sellers":             f"{SILVER_BASE}silver_sellers",
    "geolocation":         f"{SILVER_BASE}silver_geolocation",
    "reviews":             f"{SILVER_BASE}silver_reviews",
}

QA_PATHS = {
    "orders":              f"{QA_BASE}silver_orders_delta",
    "order_items":         f"{QA_BASE}silver_order_items_delta",
    "products":            f"{QA_BASE}silver_products_delta",
    "customers":           f"{QA_BASE}silver_customers_delta",
    "payments":            f"{QA_BASE}silver_payments_delta",
    "sellers":             f"{QA_BASE}silver_sellers_delta",
    "geolocation":         f"{QA_BASE}silver_geolocation_delta",
    "reviews":             f"{QA_BASE}silver_reviews_delta",
}

INTEGRITY_ERRORS_PATH = f"{QA_BASE}silver_customers_integrity_errors_delta"

# =============================================================================
# CONSTANTS
# =============================================================================
BRAZIL_LAT_MIN = -34.0
BRAZIL_LAT_MAX =   6.0
BRAZIL_LNG_MIN = -75.0
BRAZIL_LNG_MAX = -28.0

BRAZIL_STATES_MAP = {
    'SP': 'São Paulo',          'MG': 'Minas Gerais',
    'RJ': 'Rio de Janeiro',     'RS': 'Rio Grande do Sul',
    'PR': 'Paraná',             'SC': 'Santa Catarina',
    'BA': 'Bahia',              'GO': 'Goiás',
    'PE': 'Pernambuco',         'ES': 'Espírito Santo',
    'CE': 'Ceará',              'MT': 'Mato Grosso',
    'DF': 'Distrito Federal',   'MS': 'Mato Grosso do Sul',
    'PA': 'Pará',               'MA': 'Maranhão',
    'PB': 'Paraíba',            'RN': 'Rio Grande do Norte',
    'PI': 'Piauí',              'AL': 'Alagoas',
    'TO': 'Tocantins',          'SE': 'Sergipe',
    'RO': 'Rondônia',           'AM': 'Amazonas',
    'AC': 'Acre',               'AP': 'Amapá',
    'RR': 'Roraima',
}

# =============================================================================
# 1. SPARK SESSION
# =============================================================================
def get_spark_session() -> SparkSession:
    """Initialize SparkSession with Delta Lake support."""
    spark = (
        SparkSession.builder
        .appName("Unified_Silver_Refining_Pipeline")
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession initialized successfully.")
    return spark


# =============================================================================
# 2. HELPER FUNCTIONS
# =============================================================================

def append_to_audit_log(spark: SparkSession, new_errors: DataFrame, audit_path: str) -> None:
    """
    Append errors to audit log using unionByName with allowMissingColumns=True.
    Preserves existing audit records and adds new ones idempotently.
    
    FIX: Removes metadata columns (_ingested_at, _source_file) from error DataFrames
    to prevent Delta duplicate column errors during write.
    """
    if new_errors.count() == 0:
        log.info(f"No new errors to append to {audit_path}")
        return
    
    # CRITICAL FIX: Drop metadata columns that come from Bronze tables
    # These columns (_ingested_at, _source_file) cause duplicate column errors
    # when unioning with existing audit logs that don't have them.
    metadata_columns = ["_ingested_at", "_source_file"]
    columns_to_drop = [col for col in metadata_columns if col in new_errors.columns]
    
    if columns_to_drop:
        new_errors = new_errors.drop(*columns_to_drop)
        log.info(f"Dropped metadata columns from audit records: {columns_to_drop}")
    
    # Ensure error_reason and error_detected_at are present
    if "error_reason" not in new_errors.columns:
        new_errors = new_errors.withColumn("error_reason", F.lit(None).cast("string"))
    if "error_detected_at" not in new_errors.columns:
        new_errors = new_errors.withColumn("error_detected_at", F.current_timestamp())
    
    try:
        existing_audit = spark.read.format("delta").load(audit_path)
        
        # Also drop metadata columns from existing audit if they exist (defensive)
        existing_columns_to_drop = [col for col in metadata_columns if col in existing_audit.columns]
        if existing_columns_to_drop:
            existing_audit = existing_audit.drop(*existing_columns_to_drop)
        
        combined_audit = existing_audit.unionByName(new_errors, allowMissingColumns=True)
        log.info(f"Read existing audit log with {existing_audit.count():,} rows")
        
    except Exception as e:
        # No existing audit data — first run
        log.info(f"No existing audit log found at {audit_path}. Creating new audit log.")
        combined_audit = new_errors
    
    # Write the combined audit log
    combined_audit.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(audit_path)
    
    log.info(f"Appended {new_errors.count():,} errors to audit log: {audit_path}")


def load_bronze_delta(spark: SparkSession, path: str, table_name: str) -> DataFrame:
    """Read a Delta table from Bronze layer."""
    log.info(f"[PHASE 1] Loading Bronze Delta: {table_name} from {path}")
    df = spark.read.format("delta").load(path)
    log.info(f"[PHASE 1] Loaded {table_name}: {df.count():,} rows")
    return df


def load_data(df: DataFrame, path: str, description: str = "DataFrame") -> None:
    """Writes a Spark DataFrame to the target Delta path with overwrite mode."""
    log.info(f"[LOAD] Writing {description} to: {path}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    log.info(f"[LOAD] {description} successfully written")


# =============================================================================
# 3. PHASE 2: INDEPENDENT SILVER REFINING
# =============================================================================

# -----------------------------------------------------------------------------
# 3.1 silver_geolocation (No dependencies)
# -----------------------------------------------------------------------------
def build_silver_geolocation(df_geo_raw: DataFrame, spark: SparkSession) -> DataFrame:
    """Refines raw geolocation data with FULL error capture."""
    log.info("[PHASE 2] Building silver_geolocation")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_geo_raw.columns:
            df_geo_raw = df_geo_raw.drop(meta_col)
    
    # Stage 3a: Initial schema cast
    df_geo = df_geo_raw \
        .withColumn("geolocation_zip_code_prefix", F.col("geolocation_zip_code_prefix").cast(IntegerType())) \
        .withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType())) \
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType())) \
        .withColumn("geolocation_city", F.col("geolocation_city").cast(StringType())) \
        .withColumn("geolocation_state", F.col("geolocation_state").cast(StringType()))
    
    # Stage 3b: Full-row deduplication
    df_geo = df_geo.dropDuplicates()
    
    # Stage 3c: Text standardization
    df_geo = df_geo.withColumn("geolocation_city", F.trim(F.lower(F.col("geolocation_city"))))
    df_geo = df_geo.withColumn("geolocation_city", F.translate(F.col("geolocation_city"), "ãáâéíóôúç", "aaaeioouc"))
    
    # Stage 3d: Window voting mechanism
    city_counts = df_geo.groupBy("geolocation_zip_code_prefix", "geolocation_city").count()
    window_spec = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("count"))
    df_ranked_cities = city_counts.withColumn("rank", F.row_number().over(window_spec))
    df_standardized_mapping = df_ranked_cities.filter(F.col("rank") == 1).select(
        "geolocation_zip_code_prefix", F.col("geolocation_city").alias("standardized_city")
    )
    df_geo = df_geo.join(df_standardized_mapping, on="geolocation_zip_code_prefix", how="inner")
    
    # Stage 3e-3g: Spatial outlier detection and imputation WITH ERROR CAPTURE
    # Capture spatial outliers BEFORE imputation
    spatial_errors = df_geo.filter(
        (F.col("geolocation_lat") < BRAZIL_LAT_MIN) | (F.col("geolocation_lat") > BRAZIL_LAT_MAX) |
        (F.col("geolocation_lng") < BRAZIL_LNG_MIN) | (F.col("geolocation_lng") > BRAZIL_LNG_MAX)
    ).withColumn("error_reason", F.lit("Spatial Outlier: Coordinates located outside Brazil's official geographic bounding box")) \
     .withColumn("error_detected_at", F.current_timestamp())
    
    append_to_audit_log(spark, spatial_errors, QA_PATHS["geolocation"])
    
    # Imputation
    df_zip_means = df_geo.filter(
        (F.col("geolocation_lat") >= BRAZIL_LAT_MIN) & (F.col("geolocation_lat") <= BRAZIL_LAT_MAX) &
        (F.col("geolocation_lng") >= BRAZIL_LNG_MIN) & (F.col("geolocation_lng") <= BRAZIL_LNG_MAX)
    ).groupBy("geolocation_zip_code_prefix").agg(
        F.mean("geolocation_lat").alias("mean_lat"), F.mean("geolocation_lng").alias("mean_lng")
    )
    df_geo = df_geo.join(df_zip_means, on="geolocation_zip_code_prefix", how="left")
    df_geo = df_geo.withColumn("geolocation_lat",
        F.when((F.col("geolocation_lat") < BRAZIL_LAT_MIN) | (F.col("geolocation_lat") > BRAZIL_LAT_MAX), F.col("mean_lat"))
         .otherwise(F.col("geolocation_lat"))
    ).withColumn("geolocation_lng",
        F.when((F.col("geolocation_lng") < BRAZIL_LNG_MIN) | (F.col("geolocation_lng") > BRAZIL_LNG_MAX), F.col("mean_lng"))
         .otherwise(F.col("geolocation_lng"))
    ).drop("mean_lat", "mean_lng")
    
    # Stage 3i: State abbreviation enrichment
    mapping_expr = F.create_map([F.lit(x) for x in chain(*BRAZIL_STATES_MAP.items())])
    df_geo = df_geo.withColumn("geolocation_state_full", F.coalesce(mapping_expr[F.col("geolocation_state")], F.col("geolocation_state")))
    
    # Schema realignment
    df_geo = df_geo.drop("geolocation_state", "geolocation_city")
    df_geo = df_geo.withColumnRenamed("standardized_city", "geolocation_city").withColumnRenamed("geolocation_state_full", "geolocation_state")
    
    df_geo_final = df_geo.select(
        F.col("geolocation_zip_code_prefix").cast(IntegerType()),
        F.col("geolocation_lat").cast(DoubleType()),
        F.col("geolocation_lng").cast(DoubleType()),
        F.col("geolocation_city").cast(StringType()),
        F.col("geolocation_state").cast(StringType()),
    )
    
    log.info(f"[PHASE 2] silver_geolocation complete: {df_geo_final.count():,} rows")
    return df_geo_final


# -----------------------------------------------------------------------------
# 3.2 silver_products (Depends only on category_translation) WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_products(df_products_raw: DataFrame, df_translation: DataFrame, spark: SparkSession) -> DataFrame:
    """Refines raw products data with FULL error capture."""
    log.info("[PHASE 2] Building silver_products")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_products_raw.columns:
            df_products_raw = df_products_raw.drop(meta_col)
        if meta_col in df_translation.columns:
            df_translation = df_translation.drop(meta_col)
    
    # Stage 1: Initial strict type casting
    df_products = df_products_raw \
        .withColumn("product_id", F.col("product_id").cast(StringType())) \
        .withColumn("product_category_name", F.col("product_category_name").cast(StringType())) \
        .withColumn("product_name_lenght", F.col("product_name_lenght").cast(IntegerType())) \
        .withColumn("product_description_lenght", F.col("product_description_lenght").cast(IntegerType())) \
        .withColumn("product_photos_qty", F.col("product_photos_qty").cast(IntegerType())) \
        .withColumn("product_weight_g", F.col("product_weight_g").cast(DecimalType(10, 2))) \
        .withColumn("product_length_cm", F.col("product_length_cm").cast(DecimalType(10, 2))) \
        .withColumn("product_height_cm", F.col("product_height_cm").cast(DecimalType(10, 2))) \
        .withColumn("product_width_cm", F.col("product_width_cm").cast(DecimalType(10, 2)))
    
    # Stage 2-3: Initialize audit and capture duplicates
    total_count = df_products.count()
    distinct_df = df_products.distinct()
    duplicates_df = df_products.subtract(distinct_df)
    
    if duplicates_df.count() > 0:
        duplicate_errors = duplicates_df.withColumn("error_reason", F.lit("Duplicate record found - Keeping only one instance")) \
                                        .withColumn("error_detected_at", F.current_timestamp())
        append_to_audit_log(spark, duplicate_errors, QA_PATHS["products"])
    
    df_products = distinct_df
    
    # Stage 4: Capture missing metadata BEFORE imputation
    missing_cond = (F.col("product_weight_g").isNull() | F.col("product_length_cm").isNull() |
                    F.col("product_height_cm").isNull() | F.col("product_width_cm").isNull() |
                    F.col("product_category_name").isNull() | F.col("product_name_lenght").isNull() |
                    F.col("product_description_lenght").isNull() | F.col("product_photos_qty").isNull())
    
    missing_errors = df_products.filter(missing_cond).withColumn("error_reason", F.lit("Missing physical dimensions, weight, or descriptive metadata")) \
                                 .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, missing_errors, QA_PATHS["products"])
    
    # Stage 5: Category-partitioned median imputation
    window_category = Window.partitionBy("product_category_name")
    df_products = df_products \
        .withColumn("product_weight_g", F.coalesce(F.col("product_weight_g"), F.percentile_approx("product_weight_g", 0.5).over(window_category)).cast(DecimalType(10, 2))) \
        .withColumn("product_length_cm", F.coalesce(F.col("product_length_cm"), F.percentile_approx("product_length_cm", 0.5).over(window_category)).cast(DecimalType(10, 2))) \
        .withColumn("product_height_cm", F.coalesce(F.col("product_height_cm"), F.percentile_approx("product_height_cm", 0.5).over(window_category)).cast(DecimalType(10, 2))) \
        .withColumn("product_width_cm", F.coalesce(F.col("product_width_cm"), F.percentile_approx("product_width_cm", 0.5).over(window_category)).cast(DecimalType(10, 2)))
    
    # Stage 6: Descriptive fillna
    df_products = df_products.fillna({"product_category_name": "Unknown Category", "product_name_lenght": 0, "product_description_lenght": 0, "product_photos_qty": 0})
    
    # Stage 7: Volume calculation
    df_products = df_products.withColumn("product_size_cm3",
        F.when(F.col("product_length_cm").isNull() | F.col("product_height_cm").isNull() | F.col("product_width_cm").isNull(), F.lit(None).cast("decimal(10,2)"))
         .otherwise(F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")))
    
    # Stage 8: Logistics categorization
    df_products = df_products.withColumn("logistics_size_category",
        F.when(F.col("product_size_cm3").isNull(), "Unknown Size")
         .when(F.col("product_size_cm3") <= 5000, "Small Box")
         .when(F.col("product_size_cm3") <= 20000, "Medium Box")
         .otherwise("Large Parcel"))
    df_products = df_products.withColumn("logistics_weight_category",
        F.when(F.col("product_weight_g").isNull(), "Unknown Weight")
         .when(F.col("product_weight_g") <= 2000, "Lightweight")
         .when(F.col("product_weight_g") <= 10000, "Midweight")
         .otherwise("Heavyweight"))
    
    # Stage 9-10: Three-tier translation and capture unknown categories
    df_products = df_products.withColumn("manual_translation",
        F.when(F.col("product_category_name") == "pc_gamer", "pc_gamer")
         .when(F.col("product_category_name") == "portateis_cozinha_e_preparadores_de_alimentos", "kitchen_portables_and_food_preparers")
         .otherwise(None))
    df_products = df_products.join(df_translation, on="product_category_name", how="left")
    df_products = df_products.withColumn("product_category_name_english",
        F.coalesce(F.col("manual_translation"), F.col("product_category_name_english"), F.lit("unknown"))).drop("manual_translation")
    
    # Capture unknown categories
    unknown_errors = df_products.filter(F.col("product_category_name") == "Unknown Category").withColumn("error_reason", F.lit("Product category is unknown")) \
                                .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, unknown_errors, QA_PATHS["products"])
    
    # Stage 11-12: Schema finalization
    df_products = df_products.drop("product_category_name").withColumnRenamed("product_category_name_english", "product_category_name")
    
    df_products_final = df_products.select(
        F.col("product_id").cast(StringType()), F.col("product_category_name").cast(StringType()),
        F.col("product_name_lenght").cast(IntegerType()), F.col("product_description_lenght").cast(IntegerType()),
        F.col("product_photos_qty").cast(IntegerType()), F.col("product_weight_g").cast(DecimalType(10, 2)),
        F.col("product_length_cm").cast(DecimalType(10, 2)), F.col("product_height_cm").cast(DecimalType(10, 2)),
        F.col("product_width_cm").cast(DecimalType(10, 2)), F.col("product_size_cm3").cast(DecimalType(10, 2)),
        F.col("logistics_size_category").cast(StringType()), F.col("logistics_weight_category").cast(StringType()),
    )
    
    log.info(f"[PHASE 2] silver_products complete: {df_products_final.count():,} rows")
    return df_products_final


# -----------------------------------------------------------------------------
# 3.3 silver_customers — PASS 1 (Without orders integrity flag) WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_customers_pass1(df_customers_raw: DataFrame, df_geo: DataFrame, spark: SparkSession) -> DataFrame:
    """PASS 1 of silver_customers with FULL error capture."""
    log.info("[PHASE 2 | PASS 1] Building silver_customers")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_customers_raw.columns:
            df_customers_raw = df_customers_raw.drop(meta_col)
        if meta_col in df_geo.columns:
            df_geo = df_geo.drop(meta_col)
    
    # Stage 1: Initial strict type casting
    df_customers = df_customers_raw \
        .withColumn("customer_id", F.col("customer_id").cast(StringType())) \
        .withColumn("customer_unique_id", F.col("customer_unique_id").cast(StringType())) \
        .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast(IntegerType())) \
        .withColumn("customer_city", F.col("customer_city").cast(StringType())) \
        .withColumn("customer_state", F.col("customer_state").cast(StringType()))
    
    # Stage 2: Geolocation zip code audit and imputation WITH ERROR CAPTURE
    geo_lookup = df_geo.select(F.col("geolocation_zip_code_prefix").alias("geo_zip")).dropDuplicates(["geo_zip"])
    customers_with_geo = df_customers.join(geo_lookup, df_customers.customer_zip_code_prefix == geo_lookup.geo_zip, how="left")
    
    geo_errors = customers_with_geo.filter(F.col("customer_zip_code_prefix").isNull() | F.col("geo_zip").isNull()).withColumn(
        "error_reason",
        F.when(F.col("customer_zip_code_prefix").isNull(), F.lit("Critical: customer_zip_code_prefix is NULL"))
         .otherwise(F.lit("Orphaned: zip_code not found in geolocation dataset"))
    ).withColumn("error_detected_at", F.current_timestamp()).drop("geo_zip")
    
    append_to_audit_log(spark, geo_errors, QA_PATHS["customers"])
    
    invalid_zips = [row.customer_zip_code_prefix for row in geo_errors.select("customer_zip_code_prefix").distinct().collect() if row.customer_zip_code_prefix is not None]
    df_customers = df_customers.withColumn("customer_zip_code_prefix",
        F.when(F.col("customer_zip_code_prefix").isNull() | F.col("customer_zip_code_prefix").isin(invalid_zips), F.lit(-1)).otherwise(F.col("customer_zip_code_prefix")))
    
    # Stage 3: Duplicate removal
    df_customers = df_customers.dropDuplicates()
    
    # Stage 6: State abbreviation standardization
    mapping_expr = F.create_map([F.lit(x) for x in chain(*BRAZIL_STATES_MAP.items())])
    df_customers = df_customers.withColumn("customer_state_full", F.coalesce(mapping_expr[F.col("customer_state")], F.col("customer_state"))) \
                               .drop("customer_state").withColumnRenamed("customer_state_full", "customer_state")
    
    # Stage 7: Regional enrichment
    df_customers = df_customers.withColumn("customer_region",
        F.when(F.col("customer_state").isin('São Paulo', 'Rio de Janeiro', 'Minas Gerais', 'Espírito Santo'), 'Southeast')
         .when(F.col("customer_state").isin('Paraná', 'Rio Grande do Sul', 'Santa Catarina'), 'South')
         .when(F.col("customer_state").isin('Bahia', 'Pernambuco', 'Ceará', 'Rio Grande do Norte', 'Maranhão', 'Paraíba', 'Alagoas', 'Sergipe', 'Piauí'), 'Northeast')
         .when(F.col("customer_state").isin('Mato Grosso', 'Mato Grosso do Sul', 'Goiás', 'Distrito Federal'), 'Central-West')
         .otherwise('North'))
    
    # Stage 9: Sentinel row injection
    dummy_record = Row(customer_id="-1", customer_unique_id="-1", customer_zip_code_prefix=-1, customer_city="Unknown", customer_state="Unknown", customer_region="Unknown")
    dummy_df = spark.createDataFrame([dummy_record])
    df_customers = df_customers.filter(F.col("customer_id") != "-1").unionByName(dummy_df)
    
    # Stage 10: Final schema enforcement
    df_customers_final = df_customers.select(
        F.col("customer_id").cast(StringType()), F.col("customer_unique_id").cast(StringType()),
        F.col("customer_zip_code_prefix").cast(IntegerType()), F.col("customer_city").cast(StringType()),
        F.col("customer_state").cast(StringType()), F.col("customer_region").cast(StringType()),
    )
    
    log.info(f"[PHASE 2 | PASS 1] silver_customers complete: {df_customers_final.count():,} rows")
    return df_customers_final


# -----------------------------------------------------------------------------
# 3.4 silver_orders — PASS 1 WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_orders_pass1(df_orders_raw: DataFrame, df_customers: DataFrame, spark: SparkSession) -> DataFrame:
    """PASS 1 of silver_orders with FULL error capture."""
    log.info("[PHASE 2 | PASS 1] Building silver_orders")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_orders_raw.columns:
            df_orders_raw = df_orders_raw.drop(meta_col)
        if meta_col in df_customers.columns:
            df_customers = df_customers.drop(meta_col)
    
    # Stage 1: Initial schema casting
    df_orders = df_orders_raw
    for col_name, data_type in [("order_id", StringType()), ("customer_id", StringType()), ("order_status", StringType()),
                                  ("order_purchase_timestamp", TimestampType()), ("order_approved_at", TimestampType()),
                                  ("order_delivered_carrier_date", TimestampType()), ("order_delivered_customer_date", TimestampType()),
                                  ("order_estimated_delivery_date", TimestampType())]:
        if col_name in df_orders.columns:
            df_orders = df_orders.withColumn(col_name, F.col(col_name).cast(data_type))
    
    # Stage 2: Orphaned customer_id detection WITH ERROR CAPTURE
    orphaned_customers = df_orders.select("customer_id").distinct().join(df_customers.select("customer_id"), "customer_id", "left_anti")
    orphaned_customer_ids = [row["customer_id"] for row in orphaned_customers.collect() if row["customer_id"] is not None]
    
    orphan_errors = df_orders.filter(F.col("customer_id").isNull() | F.col("customer_id").isin(orphaned_customer_ids)).withColumn(
        "error_reason", F.lit("Orphaned or Null customer_id - Imputed with -1")
    ).withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, orphan_errors, QA_PATHS["orders"])
    
    df_orders = df_orders.withColumn("customer_id",
        F.when(F.col("customer_id").isNull() | F.col("customer_id").isin(orphaned_customer_ids), F.lit("-1")).otherwise(F.col("customer_id")))
    
    # Stage 3: Missing timestamp imputation
    valid_timestamps_df = df_orders.filter(
        F.col("order_purchase_timestamp").isNotNull() & F.col("order_approved_at").isNotNull() &
        F.col("order_delivered_carrier_date").isNotNull() & F.col("order_delivered_customer_date").isNotNull() &
        F.col("order_estimated_delivery_date").isNotNull()
    ).select(
        (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")).alias("p_to_a"),
        (F.unix_timestamp("order_delivered_carrier_date") - F.unix_timestamp("order_approved_at")).alias("a_to_c"),
        (F.unix_timestamp("order_delivered_customer_date") - F.unix_timestamp("order_delivered_carrier_date")).alias("c_to_cust"),
        (F.unix_timestamp("order_estimated_delivery_date") - F.unix_timestamp("order_purchase_timestamp")).alias("p_to_e"),
    )
    medians = valid_timestamps_df.approxQuantile(["p_to_a", "a_to_c", "c_to_cust", "p_to_e"], [0.5], 0.01)
    med_p_to_a = int(medians[0][0]) if (medians and medians[0]) else 3600
    med_a_to_c = int(medians[1][0]) if (medians and medians[1]) else 86400
    med_c_to_cust = int(medians[2][0]) if (medians and medians[2]) else 259200
    med_p_to_e = int(medians[3][0]) if (medians and medians[3]) else 950400
    
    # Capture timestamp errors BEFORE imputation
    timestamp_errors = df_orders.filter(
        (F.col("order_status") == "delivered") & (
            F.col("order_purchase_timestamp").isNull() | F.col("order_approved_at").isNull() |
            F.col("order_delivered_carrier_date").isNull() | F.col("order_delivered_customer_date").isNull()
        )
    ).withColumn("error_reason", F.lit("Delivered status with missing timestamps - Fixed via Sequential Imputation")) \
     .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, timestamp_errors, QA_PATHS["orders"])
    
    # Sequential imputation
    df_orders = df_orders.withColumn("order_purchase_timestamp",
        F.when(F.col("order_purchase_timestamp").isNotNull(), F.col("order_purchase_timestamp"))
         .when(F.col("order_approved_at").isNotNull(), F.from_unixtime(F.unix_timestamp("order_approved_at") - med_p_to_a).cast("timestamp"))
         .when(F.col("order_delivered_carrier_date").isNotNull(), F.from_unixtime(F.unix_timestamp("order_delivered_carrier_date") - (med_p_to_a + med_a_to_c)).cast("timestamp"))
         .when(F.col("order_delivered_customer_date").isNotNull(), F.from_unixtime(F.unix_timestamp("order_delivered_customer_date") - (med_p_to_a + med_a_to_c + med_c_to_cust)).cast("timestamp"))
         .otherwise(F.from_unixtime(F.unix_timestamp("order_estimated_delivery_date") - med_p_to_e).cast("timestamp")))
    df_orders = df_orders.withColumn("order_approved_at",
        F.when(F.col("order_approved_at").isNotNull(), F.col("order_approved_at"))
         .otherwise(F.from_unixtime(F.unix_timestamp("order_purchase_timestamp") + med_p_to_a).cast("timestamp")))
    df_orders = df_orders.withColumn("order_delivered_carrier_date",
        F.when(F.col("order_delivered_carrier_date").isNotNull(), F.col("order_delivered_carrier_date"))
         .otherwise(F.from_unixtime(F.unix_timestamp("order_approved_at") + med_a_to_c).cast("timestamp")))
    df_orders = df_orders.withColumn("order_delivered_customer_date",
        F.when(F.col("order_delivered_customer_date").isNotNull(), F.col("order_delivered_customer_date"))
         .otherwise(F.from_unixtime(F.unix_timestamp("order_delivered_carrier_date") + med_c_to_cust).cast("timestamp")))
    
    # Stage 4: Chronological violation detection and correction WITH ERROR CAPTURE
    chronology_errors = df_orders.filter(
        (F.col("order_approved_at") < F.col("order_purchase_timestamp")) |
        (F.col("order_delivered_carrier_date") < F.col("order_approved_at")) |
        (F.col("order_delivered_carrier_date") < F.col("order_purchase_timestamp")) |
        (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date")) |
        (F.col("order_delivered_customer_date") < F.col("order_approved_at")) |
        (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"))
    ).withColumn("error_reason", F.lit("Invalid date chronology (Olist logic violation)")) \
     .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, chronology_errors, QA_PATHS["orders"])
    
    df_orders = df_orders.withColumn("order_approved_at",
        F.when(F.col("order_approved_at") < F.col("order_purchase_timestamp"), F.col("order_purchase_timestamp")).otherwise(F.col("order_approved_at")))
    df_orders = df_orders.withColumn("order_delivered_carrier_date",
        F.when(F.col("order_delivered_carrier_date") < F.col("order_approved_at"), F.col("order_approved_at")).otherwise(F.col("order_delivered_carrier_date")))
    df_orders = df_orders.withColumn("order_delivered_customer_date",
        F.when(F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date"), F.col("order_delivered_carrier_date")).otherwise(F.col("order_delivered_customer_date")))
    
    # Stage 5: Business enrichment columns
    df_orders = df_orders \
        .withColumn("handling_days", F.datediff("order_delivered_carrier_date", "order_approved_at")) \
        .withColumn("shipping_days", F.datediff("order_delivered_customer_date", "order_delivered_carrier_date")) \
        .withColumn("total_lead_time", F.datediff("order_delivered_customer_date", "order_purchase_timestamp")) \
        .withColumn("days_diff_estimated", F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")) \
        .withColumn("estimated_buffer", F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
    
    df_orders = df_orders.withColumn("delivery_status_detail",
        F.when(F.col("order_status") == "canceled", "Canceled")
         .when(F.col("order_status") == "unavailable", "Unavailable")
         .when(F.col("order_status").isin("shipped", "processing", "approved", "created", "invoiced"), "In Progress")
         .when(F.col("order_status") == "delivered",
              F.when(F.col("days_diff_estimated") < 0, "Early")
               .when(F.col("days_diff_estimated") == 0, "On Time")
               .when(F.col("days_diff_estimated") > 0, "Late")
               .otherwise(None))
         .otherwise("Other"))
    df_orders = df_orders.withColumn("abs_days_diff",
        F.when((F.col("order_status") == "delivered") & (F.col("days_diff_estimated").isNotNull()), F.abs(F.col("days_diff_estimated"))).otherwise(F.lit(None)))
    df_orders = df_orders.withColumn("on_time_flag",
        F.when((F.col("order_status") == "delivered") & (F.col("days_diff_estimated") <= 0), 1).otherwise(0).cast(IntegerType()))
    
    # Stage 6: Sentinel row injection
    unknown_order_values = [("-1", "-1", "unknown", None, None, None, None, None, 0, 0, 0, 0, 0, "Unknown Order", 0, 0)]
    df_unknown_row = spark.createDataFrame(unknown_order_values, schema=df_orders.select("order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "handling_days", "shipping_days", "total_lead_time", "days_diff_estimated",
        "estimated_buffer", "delivery_status_detail", "abs_days_diff", "on_time_flag").schema)
    df_orders = df_orders.filter(F.col("order_id") != "-1").unionByName(df_unknown_row)
    
    df_orders_final = df_orders.select(
        F.col("order_id").cast(StringType()), F.col("customer_id").cast(StringType()), F.col("order_status").cast(StringType()),
        F.col("order_purchase_timestamp").cast(TimestampType()), F.col("order_approved_at").cast(TimestampType()),
        F.col("order_delivered_carrier_date").cast(TimestampType()), F.col("order_delivered_customer_date").cast(TimestampType()),
        F.col("order_estimated_delivery_date").cast(TimestampType()), F.col("handling_days").cast(IntegerType()),
        F.col("shipping_days").cast(IntegerType()), F.col("total_lead_time").cast(IntegerType()),
        F.col("days_diff_estimated").cast(IntegerType()), F.col("estimated_buffer").cast(IntegerType()),
        F.col("delivery_status_detail").cast(StringType()), F.col("abs_days_diff").cast(IntegerType()),
        F.col("on_time_flag").cast(IntegerType())
    )
    
    log.info(f"[PHASE 2 | PASS 1] silver_orders complete: {df_orders_final.count():,} rows")
    return df_orders_final


# -----------------------------------------------------------------------------
# 3.5 silver_order_items WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_order_items(df_items_raw: DataFrame, df_orders: DataFrame, df_products: DataFrame, df_sellers_raw: DataFrame, spark: SparkSession) -> DataFrame:
    """Refines order items with FULL error capture."""
    log.info("[PHASE 2] Building silver_order_items")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_items_raw.columns:
            df_items_raw = df_items_raw.drop(meta_col)
        if meta_col in df_orders.columns:
            df_orders = df_orders.drop(meta_col)
        if meta_col in df_products.columns:
            df_products = df_products.drop(meta_col)
        if meta_col in df_sellers_raw.columns:
            df_sellers_raw = df_sellers_raw.drop(meta_col)
    
    df_items = df_items_raw
    
    # Stage 1-3: Orphan detection and imputation WITH ERROR CAPTURE
    orders_lookup = df_orders.select(F.col("order_id").alias("valid_order_id")).distinct()
    items_with_orders = df_items.join(orders_lookup, F.col("order_id") == F.col("valid_order_id"), "left_outer")
    orphan_orders = items_with_orders.filter(F.col("valid_order_id").isNull()).withColumn("error_reason", F.lit("Orphaned item: parent order_id was missing in Silver Orders - Imputed with -1")) \
                                     .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, orphan_orders.drop("valid_order_id"), QA_PATHS["order_items"])
    
    df_items = items_with_orders.withColumn("order_id", F.when(F.col("valid_order_id").isNotNull(), F.col("order_id")).otherwise(F.lit("-1"))).drop("valid_order_id")
    
    products_lookup = df_products.select(F.col("product_id").alias("valid_product_id")).distinct()
    items_with_products = df_items.join(products_lookup, F.col("product_id") == F.col("valid_product_id"), "left_outer")
    orphan_products = items_with_products.filter(F.col("valid_product_id").isNull()).withColumn("error_reason", F.lit("Orphaned item: product_id not found in Products - Imputed with -1")) \
                                         .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, orphan_products.drop("valid_product_id"), QA_PATHS["order_items"])
    
    df_items = items_with_products.withColumn("product_id", F.when(F.col("valid_product_id").isNotNull(), F.col("product_id")).otherwise(F.lit("-1"))).drop("valid_product_id")
    
    sellers_lookup = df_sellers_raw.select(F.col("seller_id").alias("valid_seller_id")).distinct()
    items_with_sellers = df_items.join(sellers_lookup, F.col("seller_id") == F.col("valid_seller_id"), "left_outer")
    orphan_sellers = items_with_sellers.filter(F.col("valid_seller_id").isNull()).withColumn("error_reason", F.lit("Orphaned item: seller_id not found in Sellers - Imputed with -1")) \
                                       .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, orphan_sellers.drop("valid_seller_id"), QA_PATHS["order_items"])
    
    df_items = items_with_sellers.withColumn("seller_id", F.when(F.col("valid_seller_id").isNotNull(), F.col("seller_id")).otherwise(F.lit("-1"))).drop("valid_seller_id")
    
    # Stage 4: Type casting
    df_items = df_items \
        .withColumn("order_id", F.col("order_id").cast(StringType())) \
        .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType())) \
        .withColumn("product_id", F.col("product_id").cast(StringType())) \
        .withColumn("seller_id", F.col("seller_id").cast(StringType())) \
        .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast(TimestampType())) \
        .withColumn("price", F.col("price").cast(DecimalType(10, 2))) \
        .withColumn("freight_value", F.col("freight_value").cast(DecimalType(10, 2)))
    
    # Stage 5-6: Shipping limit date median and imputation WITH ERROR CAPTURE
    items_with_dates = df_items.join(df_orders.select("order_id", "order_purchase_timestamp", "order_approved_at"), on="order_id", how="inner")
    valid_records = items_with_dates.filter(F.col("shipping_limit_date").isNotNull() & F.col("order_approved_at").isNotNull() & (F.col("shipping_limit_date") >= F.col("order_approved_at")))
    median_row = valid_records.select(F.percentile_approx(F.datediff("shipping_limit_date", "order_approved_at"), 0.5).alias("median_days")).collect()
    calculated_median_days = int(median_row[0]["median_days"]) if median_row and median_row[0]["median_days"] is not None else 4
    
    shipping_errors = items_with_dates.filter(F.col("shipping_limit_date").isNull() | (F.col("shipping_limit_date") < F.col("order_purchase_timestamp")) | (F.col("shipping_limit_date") < F.col("order_approved_at"))) \
        .withColumn("error_reason",
            F.when(F.col("shipping_limit_date").isNull(), F.lit("Missing shipping_limit_date (Null) - Rebuilt using fallback data-driven logic"))
             .otherwise(F.lit(f"Shipping limit date is before purchase/approval date - Imputed using calculated median of {calculated_median_days} days"))) \
        .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, shipping_errors, QA_PATHS["order_items"])
    
    items_refined = items_with_dates.withColumn("shipping_limit_date",
        F.when(F.col("shipping_limit_date").isNull() | (F.col("shipping_limit_date") < F.col("order_purchase_timestamp")) | (F.col("shipping_limit_date") < F.col("order_approved_at")),
               F.coalesce(F.date_add(F.col("order_approved_at"), calculated_median_days), F.date_add(F.col("order_purchase_timestamp"), calculated_median_days)))
         .otherwise(F.col("shipping_limit_date"))
    ).drop("order_purchase_timestamp", "order_approved_at")
    
    # Stage 7: Seller performance enrichment
    shipping_reference = df_orders.select("order_id", "order_status", "order_delivered_carrier_date")
    items_joined = items_refined.join(shipping_reference, on="order_id", how="left")
    items_final = items_joined \
        .withColumn("seller_handling_days", F.datediff("order_delivered_carrier_date", "shipping_limit_date")) \
        .withColumn("seller_performance",
            F.when(F.col("order_status") == "canceled", "Canceled")
             .when(F.col("order_status") == "unavailable", "Unavailable")
             .when(F.col("order_status").isin("shipped", "processing", "approved", "created"), "In Progress")
             .when(F.col("order_status") == "delivered",
                  F.when(F.col("seller_handling_days") < 0, "Early")
                   .when(F.col("seller_handling_days") == 0, "On Time")
                   .when(F.col("seller_handling_days") > 0, "Late Delivery to Carrier")
                   .otherwise("Unfulfilled"))
             .otherwise("Unknown")
        ) \
        .withColumn("abs_seller_handling",
            F.when((F.col("order_status") == "delivered") & (F.col("seller_handling_days").isNotNull()), F.abs(F.col("seller_handling_days"))).otherwise(F.lit(None))
        ).drop("order_delivered_carrier_date", "order_status")
    
    # Stage 8: Final schema
    df_items_final = items_final.select(
        F.col("order_id").cast(StringType()), F.col("order_item_id").cast(IntegerType()),
        F.col("product_id").cast(StringType()), F.col("seller_id").cast(StringType()),
        F.col("shipping_limit_date").cast(TimestampType()), F.col("price").cast(DecimalType(10, 2)),
        F.col("freight_value").cast(DecimalType(10, 2)), F.col("seller_handling_days").cast(IntegerType()),
        F.col("abs_seller_handling").cast(IntegerType()), F.col("seller_performance").cast(StringType()),
    )
    
    log.info(f"[PHASE 2] silver_order_items complete: {df_items_final.count():,} rows")
    return df_items_final


# -----------------------------------------------------------------------------
# 3.6 silver_payments WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_payments(df_payments_raw: DataFrame, df_orders: DataFrame, spark: SparkSession) -> DataFrame:
    """Refines payments with FULL error capture."""
    log.info("[PHASE 2] Building silver_payments")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_payments_raw.columns:
            df_payments_raw = df_payments_raw.drop(meta_col)
        if meta_col in df_orders.columns:
            df_orders = df_orders.drop(meta_col)
    
    # Orphan detection WITH ERROR CAPTURE
    payments_with_orders = df_payments_raw.join(df_orders.select("order_id"), on="order_id", how="left")
    payment_errors = payments_with_orders.filter(F.col("order_id").isNull()).withColumn("error_reason", F.lit("Critical: order_id is NULL")) \
                                          .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, payment_errors, QA_PATHS["payments"])
    
    df_payments = payments_with_orders.withColumn("order_id", F.when(F.col("order_id").isNull(), F.lit("-1")).otherwise(F.col("order_id")))
    
    # Deduplication
    df_payments = df_payments.dropDuplicates()
    
    # Credit card zero-installment imputation
    df_payments = df_payments.withColumn("payment_installments",
        F.when((F.col("payment_type") == "credit_card") & (F.col("payment_installments") == 0), F.lit(1)).otherwise(F.col("payment_installments")))
    
    # Feature engineering
    df_payments = df_payments.withColumn("is_installment_payment", F.when(F.col("payment_installments") > 1, 1).otherwise(0).cast(IntegerType()))
    
    # Final schema
    df_payments_final = df_payments.select(
        F.col("order_id").cast(StringType()), F.col("payment_sequential").cast(IntegerType()),
        F.col("payment_type").cast(StringType()), F.col("payment_installments").cast(IntegerType()),
        F.col("payment_value").cast(DecimalType(10, 2)), F.col("is_installment_payment").cast(IntegerType()),
    )
    
    log.info(f"[PHASE 2] silver_payments complete: {df_payments_final.count():,} rows")
    return df_payments_final


# -----------------------------------------------------------------------------
# 3.7 silver_reviews WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_reviews(df_reviews_raw: DataFrame, df_orders: DataFrame, spark: SparkSession) -> DataFrame:
    """Refines reviews with FULL error capture."""
    log.info("[PHASE 2] Building silver_reviews")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_reviews_raw.columns:
            df_reviews_raw = df_reviews_raw.drop(meta_col)
        if meta_col in df_orders.columns:
            df_orders = df_orders.drop(meta_col)
    
    # Orphan detection WITH ERROR CAPTURE
    orphaned_ids = [row.order_id for row in df_reviews_raw.join(df_orders.select("order_id"), on="order_id", how="left_anti").filter(F.col("order_id").isNotNull()).select("order_id").distinct().collect() if row.order_id is not None]
    
    null_errors = df_reviews_raw.filter(F.col("order_id").isNull()).withColumn("error_reason", F.lit("Critical: order_id is NULL")) \
                                 .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, null_errors, QA_PATHS["reviews"])
    
    orphan_errors = df_reviews_raw.filter(F.col("order_id").isin(orphaned_ids)).withColumn("error_reason", F.lit("Orphaned: order_id not found in orders_silver")) \
                                   .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, orphan_errors, QA_PATHS["reviews"])
    
    df_reviews = df_reviews_raw.withColumn("order_id",
        F.when(F.col("order_id").isNull(), F.lit("-1"))
         .when(F.col("order_id").isin(orphaned_ids), F.lit("-1"))
         .otherwise(F.col("order_id")))
    
    # Drop free-text columns
    df_reviews = df_reviews.drop("review_comment_title", "review_comment_message")
    
    # Type casting
    df_reviews = df_reviews \
        .withColumn("review_id", F.col("review_id").cast(StringType())) \
        .withColumn("order_id", F.col("order_id").cast(StringType())) \
        .withColumn("review_score", F.col("review_score").cast(IntegerType())) \
        .withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Exact duplicate removal WITH ERROR CAPTURE
    distinct_df = df_reviews.dropDuplicates()
    duplicate_errors = df_reviews.subtract(distinct_df).withColumn("error_reason", F.lit("Duplicate record found - Keeping only one instance")) \
                                 .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, duplicate_errors, QA_PATHS["reviews"])
    df_reviews = distinct_df
    
    # Logical duplicate resolution (keep latest per order)
    window_spec = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc())
    df_reviews = df_reviews.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")
    
    # Temporal imputation WITH ERROR CAPTURE
    temporal_errors = df_reviews.filter(F.col("review_creation_date").isNull() | F.col("review_answer_timestamp").isNull()) \
        .withColumn("error_reason", F.lit("Temporal Error: Missing date, imputed via Median")) \
        .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, temporal_errors, QA_PATHS["reviews"])
    
    # Phase-1 imputation
    median_stats = df_reviews.filter(F.col("review_creation_date").isNotNull() & F.col("review_answer_timestamp").isNotNull()) \
        .select(F.percentile_approx(F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date"), 0.5).alias("median_response_diff")).collect()[0]
    median_response_diff = median_stats["median_response_diff"] if median_stats["median_response_diff"] is not None else 86400
    
    df_reviews = df_reviews.withColumn("review_answer_timestamp",
        F.when(F.col("review_answer_timestamp").isNull() & F.col("review_creation_date").isNotNull(),
               F.from_unixtime(F.unix_timestamp("review_creation_date") + median_response_diff))
         .otherwise(F.col("review_answer_timestamp")))
    
    # Phase-2 cascading imputation
    df_enriched = df_reviews.join(df_orders.select("order_id", "order_purchase_timestamp"), on="order_id", how="left")
    stats = df_enriched.filter(F.col("review_creation_date").isNotNull() & F.col("order_purchase_timestamp").isNotNull() & F.col("review_answer_timestamp").isNotNull()) \
        .select(F.percentile_approx(F.unix_timestamp("review_creation_date") - F.unix_timestamp("order_purchase_timestamp"), 0.5).alias("m_p2c"),
                F.percentile_approx(F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date"), 0.5).alias("m_c2a")).collect()[0]
    m_p2c = stats["m_p2c"] if stats["m_p2c"] is not None else 604800
    m_c2a = stats["m_c2a"] if stats["m_c2a"] is not None else 86400
    
    df_reviews = df_enriched \
        .withColumn("review_creation_date",
            F.when(F.col("review_creation_date").isNull(), F.from_unixtime(F.unix_timestamp("order_purchase_timestamp") + m_p2c)).otherwise(F.col("review_creation_date"))) \
        .withColumn("review_answer_timestamp",
            F.when(F.col("review_answer_timestamp").isNull(), F.from_unixtime(F.unix_timestamp("review_creation_date") + m_c2a)).otherwise(F.col("review_answer_timestamp"))) \
        .drop("order_purchase_timestamp")
    
    # Chronological swap correction WITH ERROR CAPTURE
    swap_errors = df_reviews.filter(F.col("review_answer_timestamp") < F.col("review_creation_date")) \
        .withColumn("error_reason", F.lit("Temporal Anomaly: Answer date precedes creation date, swapped")) \
        .withColumn("error_detected_at", F.current_timestamp())
    append_to_audit_log(spark, swap_errors, QA_PATHS["reviews"])
    
    df_reviews = df_reviews \
        .withColumn("temp_creation", F.col("review_creation_date")) \
        .withColumn("review_creation_date",
            F.when(F.col("review_answer_timestamp") < F.col("review_creation_date"), F.col("review_answer_timestamp")).otherwise(F.col("review_creation_date"))) \
        .withColumn("review_answer_timestamp",
            F.when(F.col("review_answer_timestamp") < F.col("temp_creation"), F.col("temp_creation")).otherwise(F.col("review_answer_timestamp"))) \
        .drop("temp_creation")
    
    # Sentiment labeling
    df_reviews = df_reviews.withColumn("review_label",
        F.when(F.col("review_score") >= 4, "Satisfied")
         .when(F.col("review_score") == 3, "Neutral")
         .otherwise("Unsatisfied"))
    
    # Response delay enrichment
    df_reviews = df_reviews \
        .withColumn("review_response_delay_days", F.datediff("review_answer_timestamp", "review_creation_date")) \
        .withColumn("review_response_delay_hours", F.round((F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date")) / 3600, 2)) \
        .withColumn("is_same_day_response", F.when(F.datediff("review_answer_timestamp", "review_creation_date") == 0, True).otherwise(False))
    
    # Final schema
    df_reviews_final = df_reviews.select(
        F.col("review_id").cast(StringType()), F.col("order_id").cast(StringType()),
        F.col("review_score").cast(IntegerType()), F.col("review_creation_date").cast(TimestampType()),
        F.col("review_answer_timestamp").cast(TimestampType()), F.col("review_label").cast(StringType()),
        F.col("review_response_delay_days").cast(IntegerType()), F.col("review_response_delay_hours").cast(DecimalType(10, 2)),
        F.col("is_same_day_response").cast(StringType()),
    )
    
    log.info(f"[PHASE 2] silver_reviews complete: {df_reviews_final.count():,} rows")
    return df_reviews_final


# -----------------------------------------------------------------------------
# 3.8 silver_sellers WITH ERROR CAPTURE
# -----------------------------------------------------------------------------
def build_silver_sellers(df_sellers_raw: DataFrame, df_geo: DataFrame, df_items: DataFrame, spark: SparkSession) -> DataFrame:
    """Refines sellers with FULL error capture."""
    log.info("[PHASE 2] Building silver_sellers")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_sellers_raw.columns:
            df_sellers_raw = df_sellers_raw.drop(meta_col)
        if meta_col in df_geo.columns:
            df_geo = df_geo.drop(meta_col)
        if meta_col in df_items.columns:
            df_items = df_items.drop(meta_col)
    
    # Stage 1: Type casting
    df_sellers = df_sellers_raw \
        .withColumn("seller_id", F.col("seller_id").cast(StringType())) \
        .withColumn("seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast(IntegerType())) \
        .withColumn("seller_city", F.col("seller_city").cast(StringType())) \
        .withColumn("seller_state", F.col("seller_state").cast(StringType()))
    
    # Stage 2: Geolocation zip code audit WITH ERROR CAPTURE
    geo_lookup = df_geo.select(F.col("geolocation_zip_code_prefix").alias("geo_zip")).dropDuplicates(["geo_zip"])
    sellers_with_geo = df_sellers.join(geo_lookup, df_sellers.seller_zip_code_prefix == geo_lookup.geo_zip, how="left")
    
    geo_errors = sellers_with_geo.filter(F.col("seller_zip_code_prefix").isNull() | F.col("geo_zip").isNull()).withColumn(
        "error_reason",
        F.when(F.col("seller_zip_code_prefix").isNull(), F.lit("Critical: seller_zip_code_prefix is NULL"))
         .otherwise(F.lit("Orphaned: zip_code not found in geolocation dataset"))
    ).withColumn("error_detected_at", F.current_timestamp()).drop("geo_zip")
    append_to_audit_log(spark, geo_errors, QA_PATHS["sellers"])
    
    invalid_zips = [row.seller_zip_code_prefix for row in geo_errors.select("seller_zip_code_prefix").distinct().collect() if row.seller_zip_code_prefix is not None]
    df_sellers = df_sellers.withColumn("seller_zip_code_prefix",
        F.when(F.col("seller_zip_code_prefix").isNull() | F.col("seller_zip_code_prefix").isin(invalid_zips), F.lit(-1)).otherwise(F.col("seller_zip_code_prefix")))
    
    # Stage 3: Duplicate removal
    df_sellers = df_sellers.dropDuplicates()
    
    # Stage 6: State abbreviation standardization
    mapping_expr = F.create_map([F.lit(x) for x in chain(*BRAZIL_STATES_MAP.items())])
    df_sellers = df_sellers.withColumn("seller_state_full", F.coalesce(mapping_expr[F.col("seller_state")], F.col("seller_state"))) \
                           .drop("seller_state").withColumnRenamed("seller_state_full", "seller_state")
    
    # Stage 7: Seller performance aggregation
    df_seller_performance = df_items.groupBy("seller_id").agg(
        F.count("order_item_id").alias("total_items_sold"),
        F.countDistinct("order_id").alias("total_unique_orders"),
        F.sum(F.when(F.col("seller_performance") == "Early", 1).otherwise(0)).alias("early_preparations"),
        F.sum(F.when(F.col("seller_performance") == "On Time", 1).otherwise(0)).alias("on_time_preparations"),
        F.sum(F.when(F.col("seller_performance").like("Late%"), 1).otherwise(0)).alias("late_preparations"),
        F.round(F.avg("seller_handling_days"), 2).alias("avg_handling_days"),
    ).withColumn("early_ratio", F.round((F.col("early_preparations") / F.col("total_items_sold")) * 100, 2)) \
     .withColumn("on_time_ratio", F.round((F.col("on_time_preparations") / F.col("total_items_sold")) * 100, 2)) \
     .withColumn("late_ratio", F.round((F.col("late_preparations") / F.col("total_items_sold")) * 100, 2))
    
    # Stage 8: Join and fillna
    df_sellers = df_sellers.join(df_seller_performance, on="seller_id", how="left").fillna({"total_items_sold": 0, "total_unique_orders": 0})
    df_sellers = df_sellers.fillna({"early_preparations": 0, "on_time_preparations": 0, "late_preparations": 0,
                                    "avg_handling_days": 0.0, "early_ratio": 0.0, "on_time_ratio": 0.0, "late_ratio": 0.0})
    df_sellers = df_sellers.withColumn("has_sales", F.when(F.col("total_items_sold") > 0, True).otherwise(False))
    
    # Regional enrichment
    df_sellers = df_sellers.withColumn("seller_region",
        F.when(F.col("seller_state").isin('São Paulo', 'Rio de Janeiro', 'Minas Gerais', 'Espírito Santo'), 'Southeast')
         .when(F.col("seller_state").isin('Paraná', 'Rio Grande do Sul', 'Santa Catarina'), 'South')
         .when(F.col("seller_state").isin('Bahia', 'Pernambuco', 'Ceará', 'Rio Grande do Norte', 'Maranhão', 'Paraíba', 'Alagoas', 'Sergipe', 'Piauí'), 'Northeast')
         .when(F.col("seller_state").isin('Mato Grosso', 'Mato Grosso do Sul', 'Goiás', 'Distrito Federal'), 'Central-West')
         .otherwise('North'))
    
    # Stage 10: Final schema
    df_sellers_final = df_sellers.select(
        F.col("seller_id").cast(StringType()), F.col("seller_zip_code_prefix").cast(IntegerType()),
        F.col("seller_city").cast(StringType()), F.col("seller_state").cast(StringType()),
        F.col("total_items_sold").cast(LongType()), F.col("total_unique_orders").cast(LongType()),
        F.col("early_preparations").cast(LongType()), F.col("on_time_preparations").cast(LongType()),
        F.col("late_preparations").cast(LongType()), F.col("avg_handling_days").cast(DoubleType()),
        F.col("early_ratio").cast(DoubleType()), F.col("on_time_ratio").cast(DoubleType()),
        F.col("late_ratio").cast(DoubleType()), F.col("has_sales").cast(BooleanType()),
        F.col("seller_region").cast(StringType()),
    )
    
    log.info(f"[PHASE 2] silver_sellers complete: {df_sellers_final.count():,} rows")
    return df_sellers_final


# =============================================================================
# 4. PHASE 3: ENRICHMENT & 2-PASS RESOLUTION
# =============================================================================

def enrich_silver_customers_pass2(df_customers: DataFrame, df_orders: DataFrame, spark: SparkSession) -> tuple:
    """PASS 2 of silver_customers - adds integrity flag and isolates errors."""
    log.info("[PHASE 3 | PASS 2] Enriching silver_customers with orders integrity flag")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_customers.columns:
            df_customers = df_customers.drop(meta_col)
        if meta_col in df_orders.columns:
            df_orders = df_orders.drop(meta_col)
    
    orders_lookup = df_orders.select("customer_id").dropDuplicates(["customer_id"]).withColumnRenamed("customer_id", "matched_order_id")
    df_customers_with_flag = df_customers.join(orders_lookup, F.col("customer_id") == F.col("matched_order_id"), how="left")
    df_customers_with_flag = df_customers_with_flag.withColumn(
        "has_matching_order",
        F.when(F.col("matched_order_id").isNotNull(), True).otherwise(False)
    ).drop("matched_order_id")
    
    integrity_errors = df_customers_with_flag.filter(F.col("has_matching_order") == False) \
        .withColumn("error_reason", F.lit("Orphaned Transaction - Missing Order Record in Source")) \
        .withColumn("error_detected_at", F.current_timestamp())
    
    df_customers_final = df_customers_with_flag.drop("has_matching_order")
    
    log.info(f"[PHASE 3 | PASS 2] Integrity errors: {integrity_errors.count():,} rows")
    return df_customers_final, integrity_errors


def enrich_silver_orders_pass2(df_orders: DataFrame, df_items: DataFrame, df_payments: DataFrame, spark: SparkSession) -> DataFrame:
    """PASS 2 of silver_orders - back-enrich from items and payments."""
    log.info("[PHASE 3 | PASS 2] Enriching silver_orders with items and payments aggregates")
    
    # Drop metadata columns if present
    for meta_col in ["_ingested_at", "_source_file"]:
        if meta_col in df_orders.columns:
            df_orders = df_orders.drop(meta_col)
        if meta_col in df_items.columns:
            df_items = df_items.drop(meta_col)
        if meta_col in df_payments.columns:
            df_payments = df_payments.drop(meta_col)
    
    # Items aggregates
    items_aggregates = df_items.groupBy("order_id").agg(
        F.sum("price").cast(DecimalType(10, 2)).alias("total_products_price"),
        F.sum("freight_value").cast(DecimalType(10, 2)).alias("total_freight_value"),
        F.count("order_item_id").cast(IntegerType()).alias("total_items_count"),
        F.countDistinct("seller_id").cast(IntegerType()).alias("seller_count"),
    ).withColumn("total_order_cost", (F.col("total_products_price") + F.col("total_freight_value")).cast(DecimalType(10, 2))) \
     .withColumn("is_multi_seller_order", F.when(F.col("seller_count") > 1, 1).otherwise(0).cast(IntegerType()))
    
    # Payments aggregates
    window_order = Window.partitionBy("order_id")
    payments_aggregates = df_payments \
        .withColumn("total_amount_paid", F.sum("payment_value").over(window_order)) \
        .select("order_id", "total_amount_paid") \
        .dropDuplicates(["order_id"])
    
    # Join all aggregates
    df_orders_enriched = df_orders.join(items_aggregates, on="order_id", how="left")
    df_orders_enriched = df_orders_enriched.join(payments_aggregates, on="order_id", how="left")
    
    # Coalesce fallbacks
    df_orders_enriched = df_orders_enriched \
        .withColumn("total_products_price", F.coalesce(F.col("total_products_price"), F.lit(0.00).cast(DecimalType(10, 2)))) \
        .withColumn("total_freight_value", F.coalesce(F.col("total_freight_value"), F.lit(0.00).cast(DecimalType(10, 2)))) \
        .withColumn("total_order_cost", F.coalesce(F.col("total_order_cost"), F.lit(0.00).cast(DecimalType(10, 2)))) \
        .withColumn("total_items_count", F.coalesce(F.col("total_items_count"), F.lit(0).cast(IntegerType()))) \
        .withColumn("seller_count", F.coalesce(F.col("seller_count"), F.lit(0).cast(IntegerType()))) \
        .withColumn("is_multi_seller_order", F.coalesce(F.col("is_multi_seller_order"), F.lit(0).cast(IntegerType())))
    
    # Financial variance and payment status
    df_orders_enriched = df_orders_enriched \
        .withColumn("financial_variance",
            F.when(F.col("total_amount_paid").isNotNull(), F.round(F.col("total_amount_paid") - F.col("total_order_cost"), 2)).otherwise(F.lit(None))) \
        .withColumn("payment_status",
            F.when(F.col("total_amount_paid").isNull(), "no payment record")
             .when(F.col("financial_variance") > 0, "overpaid")
             .when(F.col("financial_variance") < 0, "underpaid")
             .otherwise("matched"))
    
    log.info(f"[PHASE 3 | PASS 2] silver_orders enrichment complete: {df_orders_enriched.count():,} rows")
    return df_orders_enriched


# =============================================================================
# 5. MAIN ORCHESTRATOR
# =============================================================================
def main():
    spark = None
    try:
        log.info("=" * 80)
        log.info("  UNIFIED SILVER REFINING PIPELINE (FULLY CORRECTED) — START")
        log.info("  Architecture: 3-Phase with 2-Pass Circular Dependency Resolution")
        log.info("  Error Capture: ALL 8 tables write to QA_Issues paths")
        log.info("  FIX: Metadata columns (_ingested_at, _source_file) dropped from audit logs")
        log.info("=" * 80)
        
        spark = get_spark_session()
        
        # =====================================================================
        # PHASE 1: LOAD ALL BRONZE DELTA TABLES
        # =====================================================================
        log.info("\n" + "=" * 80)
        log.info("  PHASE 1: Loading Bronze Delta Tables")
        log.info("=" * 80)
        
        bronze = {}
        for table_name, path in BRONZE_PATHS.items():
            bronze[table_name] = load_bronze_delta(spark, path, table_name)
        
        # =====================================================================
        # PHASE 2: INDEPENDENT SILVER REFINING WITH ERROR CAPTURE
        # =====================================================================
        log.info("\n" + "=" * 80)
        log.info("  PHASE 2: Independent Silver Refining (Cleaning + Error Capture)")
        log.info("=" * 80)
        
        # 2.1 silver_geolocation
        silver_geolocation = build_silver_geolocation(bronze["geolocation"], spark)
        load_data(silver_geolocation, SILVER_PATHS["geolocation"], "silver_geolocation")
        
        # 2.2 silver_products
        silver_products = build_silver_products(bronze["products"], bronze["category_translation"], spark)
        load_data(silver_products, SILVER_PATHS["products"], "silver_products")
        
        # 2.3 silver_customers — PASS 1
        silver_customers_pass1 = build_silver_customers_pass1(bronze["customers"], silver_geolocation, spark)
        
        # 2.4 silver_orders — PASS 1
        silver_orders_pass1 = build_silver_orders_pass1(bronze["orders"], silver_customers_pass1, spark)
        
        # 2.5 silver_order_items
        silver_order_items = build_silver_order_items(bronze["order_items"], silver_orders_pass1, silver_products, bronze["sellers"], spark)
        load_data(silver_order_items, SILVER_PATHS["order_items"], "silver_order_items")
        
        # 2.6 silver_payments
        silver_payments = build_silver_payments(bronze["order_payments"], silver_orders_pass1, spark)
        load_data(silver_payments, SILVER_PATHS["payments"], "silver_payments")
        
        # 2.7 silver_reviews
        silver_reviews = build_silver_reviews(bronze["order_reviews"], silver_orders_pass1, spark)
        load_data(silver_reviews, SILVER_PATHS["reviews"], "silver_reviews")
        
        # 2.8 silver_sellers
        silver_sellers = build_silver_sellers(bronze["sellers"], silver_geolocation, silver_order_items, spark)
        load_data(silver_sellers, SILVER_PATHS["sellers"], "silver_sellers")
        
        # =====================================================================
        # PHASE 3: ENRICHMENT & 2-PASS CIRCULAR DEPENDENCY RESOLUTION
        # =====================================================================
        log.info("\n" + "=" * 80)
        log.info("  PHASE 3: Enrichment & 2-Pass Resolution")
        log.info("=" * 80)
        
        # 3.1 PASS 2: Enrich silver_customers
        silver_customers_final, integrity_errors = enrich_silver_customers_pass2(silver_customers_pass1, silver_orders_pass1, spark)
        load_data(silver_customers_final, SILVER_PATHS["customers"], "silver_customers (final)")
        load_data(integrity_errors, INTEGRITY_ERRORS_PATH, "customer_integrity_errors")
        
        # 3.2 PASS 2: Enrich silver_orders
        silver_orders_final = enrich_silver_orders_pass2(silver_orders_pass1, silver_order_items, silver_payments, spark)
        load_data(silver_orders_final, SILVER_PATHS["orders"], "silver_orders (final)")
        
        # =====================================================================
        # COMPLETION SUMMARY
        # =====================================================================
        log.info("\n" + "=" * 80)
        log.info("  UNIFIED SILVER REFINING PIPELINE — COMPLETE")
        log.info("=" * 80)
        log.info("  Tables written:")
        for table, path in SILVER_PATHS.items():
            log.info(f"    ✅ {table}: {path}")
        log.info("  Error logs written:")
        for table, path in QA_PATHS.items():
            log.info(f"    📋 {table}_errors: {path}")
        log.info(f"    📋 customer_integrity_errors: {INTEGRITY_ERRORS_PATH}")
        log.info("=" * 80)
        
    except Exception as e:
        log.error(f"PIPELINE FAILED: {e}", exc_info=True)
        raise
    
    finally:
        if spark:
            spark.stop()
            log.info("SparkSession stopped.")


if __name__ == "__main__":
    main()