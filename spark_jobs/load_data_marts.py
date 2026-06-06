"""
=============================================================================
  load_data_marts.py  –  Olist Serving Layer / Power BI Data Marts
=============================================================================
Compatible with the latest Gold Layer produced by aggregate_gold.py.

Creates 8 production-friendly marts:
  1) mart_sales_analytics
  2) mart_seller_performance
  3) mart_seller_alerts
  4) mart_product_analytics
  5) mart_delivery_analytics
  6) mart_customer_analytics
  7) mart_customer_satisfaction
  8) mart_order_funnel
  9) mart_payment_analytics

Note:
- The original plan was 8 analytical areas, but seller_alerts is separated from
  seller_performance to answer Q4 cleanly without filtering the general seller mart.
- Q4 uses the last 90 days relative to the MAX purchase date in the dataset,
  because Olist is a historical dataset.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===================================================================
# 1. SPARK SESSION
# ===================================================================
spark = (SparkSession.builder
    .appName("olist-data-marts")
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

# ===================================================================
# 2. POSTGRESQL CONNECTION
# ===================================================================
PG_URL = "jdbc:postgresql://postgres-dw:5432/olist_dw"
PG_PROPS = {
    "user": "olist",
    "password": "olist",
    "driver": "org.postgresql.Driver"
}


def save_mart(df, table_name):
    """Save mart to PostgreSQL using overwrite mode."""
    count = df.count()
    print(f"   💾 Saving {table_name} ({count:,} rows)...", flush=True)
    if count > 0:
        (df.write
           .jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPS))
        print(f"   ✅ {table_name} done!", flush=True)
    else:
        print(f"   ⚠️ {table_name} has 0 rows, skipping...", flush=True)


# ===================================================================
# 3. LOAD GOLD TABLES
# ===================================================================
print("\n" + "=" * 70)
print("📂 Loading Gold tables from PostgreSQL...")
print("=" * 70)

fct_orders = spark.read.jdbc(url=PG_URL, table="fct_orders", properties=PG_PROPS)
fct_sf = spark.read.jdbc(url=PG_URL, table="fct_seller_fulfillment", properties=PG_PROPS)
fct_pay = spark.read.jdbc(url=PG_URL, table="fct_customer_payment", properties=PG_PROPS)
fct_rev = spark.read.jdbc(url=PG_URL, table="fct_customer_review", properties=PG_PROPS)
dim_customer = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)
dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)
dim_seller = spark.read.jdbc(url=PG_URL, table="dim_seller", properties=PG_PROPS)
dim_date = spark.read.jdbc(url=PG_URL, table="dim_date", properties=PG_PROPS)

print(f"✅ fct_orders: {fct_orders.count():,} rows", flush=True)
print(f"✅ fct_seller_fulfillment: {fct_sf.count():,} rows", flush=True)

# Register temp views for SQL marts.
fct_orders.createOrReplaceTempView("fct_orders")
fct_sf.createOrReplaceTempView("fct_seller_fulfillment")
fct_pay.createOrReplaceTempView("fct_customer_payment")
fct_rev.createOrReplaceTempView("fct_customer_review")
dim_customer.createOrReplaceTempView("dim_customer")
dim_product.createOrReplaceTempView("dim_product")
dim_seller.createOrReplaceTempView("dim_seller")
dim_date.createOrReplaceTempView("dim_date")

# ===================================================================
# 4. SHARED BASE VIEWS
# ===================================================================
print("\n" + "=" * 70)
print("🏗️ Building shared base views...")
print("=" * 70)

# Review at order grain to avoid duplicated review scores when joining to order items.
review_by_order = spark.sql("""
    SELECT
        order_id,
        COUNT(DISTINCT review_id) AS review_count,
        ROUND(AVG(review_score), 2) AS avg_review_score,
        SUM(CASE WHEN review_score >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
        SUM(CASE WHEN review_score = 3 THEN 1 ELSE 0 END) AS neutral_reviews,
        SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
        ROUND(AVG(review_response_delay_days), 2) AS avg_review_response_delay_days
    FROM fct_customer_review
    GROUP BY order_id
""")
review_by_order.createOrReplaceTempView("review_by_order")

# Payment at order grain to avoid duplicate order revenue when orders have multiple payment rows.
payment_by_order = spark.sql("""
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2) AS total_payment_value,
        COUNT(*) AS payment_transactions,
        MAX(CASE WHEN is_installment_payment = 1 THEN 1 ELSE 0 END) AS has_installment_payment,
        ROUND(AVG(payment_installments), 2) AS avg_installments
    FROM fct_customer_payment
    GROUP BY order_id
""")
payment_by_order.createOrReplaceTempView("payment_by_order")

# Seller-order grain: one row per seller per order.
seller_order_base = spark.sql("""
    SELECT
        sf.seller_sk_fk,
        sf.order_id,
        MIN(sf.purchase_date_sk_fk) AS purchase_date_sk_fk,
        COUNT(sf.order_item_id) AS items_sold,
        ROUND(SUM(sf.price), 2) AS seller_order_revenue,
        ROUND(SUM(sf.freight_value), 2) AS seller_order_freight,
        ROUND(AVG(sf.seller_handling_days), 2) AS avg_seller_handling_days,
        MAX(CASE WHEN sf.seller_performance = 'Late Fulfillment' THEN 1 ELSE 0 END) AS is_late_fulfillment,
        MAX(CASE WHEN sf.seller_performance = 'On Time Fulfillment' THEN 1 ELSE 0 END) AS is_on_time_fulfillment
    FROM fct_seller_fulfillment sf
    GROUP BY sf.seller_sk_fk, sf.order_id
""")
seller_order_base.createOrReplaceTempView("seller_order_base")

# Category-order grain: one row per category per order.
category_order_base = spark.sql("""
    SELECT
        p.product_category_name,
        p.logistics_size_category,
        p.logistics_weight_category,
        sf.order_id,
        MIN(sf.purchase_date_sk_fk) AS purchase_date_sk_fk,
        COUNT(sf.order_item_id) AS items_sold,
        COUNT(DISTINCT sf.seller_sk_fk) AS sellers_count,
        ROUND(SUM(sf.price), 2) AS category_order_revenue,
        ROUND(SUM(sf.freight_value), 2) AS category_order_freight,
        ROUND(AVG(sf.seller_handling_days), 2) AS avg_seller_handling_days,
        MAX(CASE WHEN sf.seller_performance = 'Late Fulfillment' THEN 1 ELSE 0 END) AS has_late_fulfillment
    FROM fct_seller_fulfillment sf
    LEFT JOIN dim_product p ON sf.product_sk_fk = p.product_sk
    GROUP BY p.product_category_name, p.logistics_size_category, p.logistics_weight_category, sf.order_id
""")
category_order_base.createOrReplaceTempView("category_order_base")

# ===================================================================
# 5. DATA MARTS
# ===================================================================
print("\n" + "=" * 70)
print("📊 Creating Serving Layer Data Marts...")
print("=" * 70)

# -------------------------------------------------------------------
# MART 1: SALES ANALYTICS
# -------------------------------------------------------------------
print("\n1️⃣ Building mart_sales_analytics...", flush=True)
mart_sales_analytics = spark.sql("""
    SELECT
        d.year_number,
        d.quarter_number,
        d.month_number,
        d.month_name,
        c.customer_state,
        c.customer_region,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        COUNT(DISTINCT fo.customer_sk_fk) AS total_customers,
        ROUND(SUM(fo.total_products_price), 2) AS products_revenue,
        ROUND(SUM(fo.total_freight_value), 2) AS freight_revenue,
        ROUND(SUM(fo.total_order_cost), 2) AS total_revenue,
        ROUND(AVG(fo.total_order_cost), 2) AS avg_order_value,
        ROUND(AVG(fo.total_items_count), 2) AS avg_items_per_order,
        SUM(CASE WHEN fo.is_multi_seller_order = 1 THEN 1 ELSE 0 END) AS multi_seller_orders,
        ROUND(100.0 * SUM(CASE WHEN fo.is_multi_seller_order = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS multi_seller_rate_pct,
        SUM(CASE WHEN fo.delivery_status_detail = 'Delivered on time' THEN 1 ELSE 0 END) AS on_time_orders,
        SUM(CASE WHEN fo.delivery_status_detail = 'Delayed' THEN 1 ELSE 0 END) AS delayed_orders,
        ROUND(100.0 * SUM(CASE WHEN fo.delivery_status_detail = 'Delivered on time' THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS on_time_rate_pct
    FROM fct_orders fo
    LEFT JOIN dim_date d ON fo.purchase_date_sk_fk = d.date_sk
    LEFT JOIN dim_customer c ON fo.customer_sk_fk = c.customer_sk
    WHERE fo.order_id IS NOT NULL
    GROUP BY d.year_number, d.quarter_number, d.month_number, d.month_name, c.customer_state, c.customer_region
    ORDER BY d.year_number, d.month_number, total_revenue DESC
""")
save_mart(mart_sales_analytics, "mart_sales_analytics")

# -------------------------------------------------------------------
# MART 2: SELLER PERFORMANCE - answers Q2 and supports Q4
# -------------------------------------------------------------------
print("\n2️⃣ Building mart_seller_performance...", flush=True)
mart_seller_performance = spark.sql("""
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_region,
        s.seller_latitude,
        s.seller_longitude,
        COUNT(DISTINCT sob.order_id) AS total_orders,
        SUM(sob.items_sold) AS total_items_sold,
        ROUND(SUM(sob.seller_order_revenue), 2) AS total_revenue,
        ROUND(SUM(sob.seller_order_freight), 2) AS total_freight,
        ROUND(AVG(sob.seller_order_revenue), 2) AS avg_revenue_per_order,
        ROUND(AVG(sob.avg_seller_handling_days), 2) AS avg_handling_days,
        SUM(sob.is_late_fulfillment) AS late_orders,
        SUM(sob.is_on_time_fulfillment) AS on_time_orders,
        ROUND(100.0 * SUM(sob.is_late_fulfillment) / COUNT(DISTINCT sob.order_id), 2) AS late_delivery_rate_pct,
        ROUND(100.0 * SUM(sob.is_on_time_fulfillment) / COUNT(DISTINCT sob.order_id), 2) AS on_time_fulfillment_rate_pct,
        ROUND(AVG(rbo.avg_review_score), 2) AS avg_review_score,
        SUM(COALESCE(rbo.positive_reviews, 0)) AS positive_reviews,
        SUM(COALESCE(rbo.negative_reviews, 0)) AS negative_reviews,
        CASE
            WHEN COUNT(DISTINCT sob.order_id) < 5 THEN 'Low Volume'
            WHEN 100.0 * SUM(sob.is_late_fulfillment) / COUNT(DISTINCT sob.order_id) > 20 THEN 'Critical Late Risk'
            WHEN 100.0 * SUM(sob.is_late_fulfillment) / COUNT(DISTINCT sob.order_id) > 10 THEN 'Late Risk'
            WHEN AVG(rbo.avg_review_score) >= 4.5 THEN 'Top Performer'
            WHEN AVG(rbo.avg_review_score) >= 4.0 THEN 'Good Performer'
            ELSE 'Average / Needs Review'
        END AS seller_performance_segment
    FROM seller_order_base sob
    LEFT JOIN dim_seller s ON sob.seller_sk_fk = s.seller_sk
    LEFT JOIN review_by_order rbo ON sob.order_id = rbo.order_id
    WHERE s.seller_id <> '-1'
    GROUP BY s.seller_id, s.seller_city, s.seller_state, s.seller_region, s.seller_latitude, s.seller_longitude
    ORDER BY total_revenue DESC
""")
save_mart(mart_seller_performance, "mart_seller_performance")

# -------------------------------------------------------------------
# MART 3: SELLER ALERTS - answers Q4 exactly
# -------------------------------------------------------------------
print("\n3️⃣ Building mart_seller_alerts...", flush=True)
mart_seller_alerts = spark.sql("""
    WITH max_date AS (
        SELECT MAX(order_purchase_timestamp) AS max_purchase_ts
        FROM fct_orders
    ),
    recent_seller_orders AS (
        SELECT
            sob.*,
            fo.order_purchase_timestamp
        FROM seller_order_base sob
        JOIN fct_orders fo ON sob.order_id = fo.order_id
        CROSS JOIN max_date md
        WHERE fo.order_purchase_timestamp >= date_sub(CAST(md.max_purchase_ts AS DATE), 90)
    )
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_region,
        COUNT(DISTINCT rso.order_id) AS orders_last_90_days,
        SUM(rso.items_sold) AS items_last_90_days,
        ROUND(SUM(rso.seller_order_revenue), 2) AS revenue_last_90_days,
        SUM(rso.is_late_fulfillment) AS late_orders_last_90_days,
        ROUND(100.0 * SUM(rso.is_late_fulfillment) / COUNT(DISTINCT rso.order_id), 2) AS late_delivery_rate_pct,
        ROUND(AVG(rso.avg_seller_handling_days), 2) AS avg_handling_days_last_90_days,
        ROUND(AVG(rbo.avg_review_score), 2) AS avg_review_score_last_90_days,
        MIN(rso.order_purchase_timestamp) AS first_order_in_window,
        MAX(rso.order_purchase_timestamp) AS last_order_in_window,
        CASE
            WHEN 100.0 * SUM(rso.is_late_fulfillment) / COUNT(DISTINCT rso.order_id) > 30 THEN 'Critical'
            WHEN 100.0 * SUM(rso.is_late_fulfillment) / COUNT(DISTINCT rso.order_id) > 20 THEN 'High'
            WHEN 100.0 * SUM(rso.is_late_fulfillment) / COUNT(DISTINCT rso.order_id) > 10 THEN 'Warning'
            ELSE 'Normal'
        END AS alert_level
    FROM recent_seller_orders rso
    LEFT JOIN dim_seller s ON rso.seller_sk_fk = s.seller_sk
    LEFT JOIN review_by_order rbo ON rso.order_id = rbo.order_id
    WHERE s.seller_id <> '-1'
    GROUP BY s.seller_id, s.seller_city, s.seller_state, s.seller_region
    HAVING COUNT(DISTINCT rso.order_id) >= 3
       AND 100.0 * SUM(rso.is_late_fulfillment) / COUNT(DISTINCT rso.order_id) > 10
    ORDER BY late_delivery_rate_pct DESC, orders_last_90_days DESC
""")
save_mart(mart_seller_alerts, "mart_seller_alerts")

# -------------------------------------------------------------------
# MART 4: PRODUCT ANALYTICS - answers Q1
# -------------------------------------------------------------------
print("\n4️⃣ Building mart_product_analytics...", flush=True)
mart_product_analytics = spark.sql("""
    SELECT
        cob.product_category_name,
        cob.logistics_size_category,
        cob.logistics_weight_category,
        COUNT(DISTINCT cob.order_id) AS total_orders,
        SUM(cob.items_sold) AS total_items_sold,
        COUNT(DISTINCT sf.seller_sk_fk) AS unique_sellers,
        ROUND(SUM(cob.category_order_revenue), 2) AS total_revenue,
        ROUND(SUM(cob.category_order_freight), 2) AS total_freight,
        ROUND(AVG(cob.category_order_revenue), 2) AS avg_category_order_value,
        ROUND(AVG(cob.avg_seller_handling_days), 2) AS avg_handling_days,
        SUM(cob.has_late_fulfillment) AS late_orders,
        ROUND(100.0 * SUM(cob.has_late_fulfillment) / COUNT(DISTINCT cob.order_id), 2) AS late_delivery_rate_pct,
        ROUND(AVG(rbo.avg_review_score), 2) AS avg_review_score,
        SUM(COALESCE(rbo.positive_reviews, 0)) AS positive_reviews,
        SUM(COALESCE(rbo.negative_reviews, 0)) AS negative_reviews,
        CASE
            WHEN SUM(cob.category_order_revenue) >= 100000 AND 100.0 * SUM(cob.has_late_fulfillment) / COUNT(DISTINCT cob.order_id) > 10 THEN 'High Revenue / Poor Delivery'
            WHEN SUM(cob.category_order_revenue) >= 100000 THEN 'High Revenue'
            WHEN 100.0 * SUM(cob.has_late_fulfillment) / COUNT(DISTINCT cob.order_id) > 10 THEN 'Poor Delivery'
            ELSE 'Normal'
        END AS category_insight_segment
    FROM category_order_base cob
    LEFT JOIN review_by_order rbo ON cob.order_id = rbo.order_id
    LEFT JOIN fct_seller_fulfillment sf ON cob.order_id = sf.order_id
    WHERE cob.product_category_name IS NOT NULL
      AND cob.product_category_name <> 'Unknown'
    GROUP BY cob.product_category_name, cob.logistics_size_category, cob.logistics_weight_category
    ORDER BY total_revenue DESC
""")
save_mart(mart_product_analytics, "mart_product_analytics")

# -------------------------------------------------------------------
# MART 5: DELIVERY ANALYTICS
# -------------------------------------------------------------------
print("\n5️⃣ Building mart_delivery_analytics...", flush=True)
mart_delivery_analytics = spark.sql("""
    SELECT
        d.year_number,
        d.quarter_number,
        d.month_number,
        d.month_name,
        c.customer_state,
        c.customer_region,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(CASE WHEN fo.delivery_status_detail = 'Delivered on time' THEN 1 ELSE 0 END) AS on_time_orders,
        SUM(CASE WHEN fo.delivery_status_detail = 'Delayed' THEN 1 ELSE 0 END) AS delayed_orders,
        ROUND(100.0 * SUM(CASE WHEN fo.delivery_status_detail = 'Delivered on time' THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS on_time_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN fo.delivery_status_detail = 'Delayed' THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS late_delivery_rate_pct,
        ROUND(AVG(fo.handling_days), 2) AS avg_handling_days,
        ROUND(AVG(fo.shipping_days), 2) AS avg_shipping_days,
        ROUND(AVG(fo.total_lead_time), 2) AS avg_total_lead_time_days,
        ROUND(AVG(fo.days_diff_estimated), 2) AS avg_days_before_estimate,
        ROUND(AVG(fo.abs_days_diff), 2) AS avg_abs_delivery_gap_days,
        ROUND(AVG(fo.estimated_buffer), 2) AS avg_estimated_buffer_days
    FROM fct_orders fo
    LEFT JOIN dim_date d ON fo.purchase_date_sk_fk = d.date_sk
    LEFT JOIN dim_customer c ON fo.customer_sk_fk = c.customer_sk
    WHERE c.customer_state <> 'Unknown'
    GROUP BY d.year_number, d.quarter_number, d.month_number, d.month_name, c.customer_state, c.customer_region
    ORDER BY d.year_number, d.month_number, total_orders DESC
""")
save_mart(mart_delivery_analytics, "mart_delivery_analytics")

# -------------------------------------------------------------------
# MART 6: CUSTOMER ANALYTICS - answers Q6
# -------------------------------------------------------------------
print("\n6️⃣ Building mart_customer_analytics...", flush=True)
mart_customer_analytics = spark.sql("""
    WITH customer_clv AS (
        SELECT
            c.customer_unique_id,
            c.customer_state,
            c.customer_region,
            COUNT(DISTINCT fo.order_id) AS total_orders,
            ROUND(SUM(fo.total_order_cost), 2) AS customer_lifetime_value,
            ROUND(AVG(fo.total_order_cost), 2) AS avg_order_value,
            MIN(fo.order_purchase_timestamp) AS first_purchase_ts,
            MAX(fo.order_purchase_timestamp) AS last_purchase_ts,
            DATEDIFF(MAX(fo.order_purchase_timestamp), MIN(fo.order_purchase_timestamp)) AS customer_lifetime_days
        FROM fct_orders fo
        LEFT JOIN dim_customer c ON fo.customer_sk_fk = c.customer_sk
        WHERE c.customer_unique_id <> '-1'
        GROUP BY c.customer_unique_id, c.customer_state, c.customer_region
    )
    SELECT
        customer_state,
        customer_region,
        COUNT(DISTINCT customer_unique_id) AS total_customers,
        SUM(total_orders) AS total_orders,
        SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
        ROUND(100.0 * SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT customer_unique_id), 2) AS repeat_customer_rate_pct,
        ROUND(SUM(customer_lifetime_value), 2) AS total_revenue,
        ROUND(AVG(customer_lifetime_value), 2) AS avg_clv,
        ROUND(AVG(total_orders), 2) AS avg_orders_per_customer,
        ROUND(AVG(avg_order_value), 2) AS avg_order_value,
        ROUND(AVG(customer_lifetime_days), 2) AS avg_customer_lifetime_days,
        SUM(CASE WHEN customer_lifetime_value >= 1000 THEN 1 ELSE 0 END) AS high_value_customers,
        SUM(CASE WHEN customer_lifetime_value >= 500 AND customer_lifetime_value < 1000 THEN 1 ELSE 0 END) AS medium_value_customers,
        SUM(CASE WHEN customer_lifetime_value < 500 THEN 1 ELSE 0 END) AS low_value_customers
    FROM customer_clv
    WHERE customer_state <> 'Unknown'
    GROUP BY customer_state, customer_region
    ORDER BY avg_clv DESC
""")
save_mart(mart_customer_analytics, "mart_customer_analytics")

# -------------------------------------------------------------------
# MART 7: CUSTOMER SATISFACTION - answers Q3
# -------------------------------------------------------------------
print("\n7️⃣ Building mart_customer_satisfaction...", flush=True)
mart_customer_satisfaction = spark.sql("""
    WITH order_satisfaction AS (
        SELECT
            fo.order_id,
            c.customer_state,
            c.customer_region,
            fo.total_freight_value,
            fo.total_order_cost,
            fo.delivery_status_detail,
            fo.total_lead_time,
            rbo.avg_review_score,
            rbo.positive_reviews,
            rbo.neutral_reviews,
            rbo.negative_reviews
        FROM fct_orders fo
        LEFT JOIN dim_customer c ON fo.customer_sk_fk = c.customer_sk
        LEFT JOIN review_by_order rbo ON fo.order_id = rbo.order_id
        WHERE c.customer_state <> 'Unknown'
          AND rbo.avg_review_score IS NOT NULL
    )
    SELECT
        customer_state,
        customer_region,
        COUNT(DISTINCT order_id) AS reviewed_orders,
        ROUND(AVG(total_freight_value), 2) AS avg_freight_cost,
        ROUND(AVG(total_order_cost), 2) AS avg_order_value,
        ROUND(AVG(avg_review_score), 2) AS avg_review_score,
        SUM(COALESCE(positive_reviews, 0)) AS positive_reviews,
        SUM(COALESCE(neutral_reviews, 0)) AS neutral_reviews,
        SUM(COALESCE(negative_reviews, 0)) AS negative_reviews,
        ROUND(100.0 * SUM(COALESCE(positive_reviews, 0)) / COUNT(DISTINCT order_id), 2) AS satisfaction_rate_pct,
        ROUND(100.0 * SUM(COALESCE(negative_reviews, 0)) / COUNT(DISTINCT order_id), 2) AS dissatisfaction_rate_pct,
        ROUND(AVG(total_lead_time), 2) AS avg_total_lead_time_days,
        ROUND(CORR(CAST(total_freight_value AS DOUBLE), CAST(avg_review_score AS DOUBLE)), 4) AS freight_review_correlation,
        CASE
            WHEN AVG(total_freight_value) >= 40 AND AVG(avg_review_score) < 4 THEN 'High Freight / Low Satisfaction'
            WHEN AVG(total_freight_value) >= 40 THEN 'High Freight'
            WHEN AVG(avg_review_score) < 4 THEN 'Low Satisfaction'
            ELSE 'Healthy'
        END AS satisfaction_segment
    FROM order_satisfaction
    GROUP BY customer_state, customer_region
    HAVING COUNT(DISTINCT order_id) >= 20
    ORDER BY avg_review_score ASC, avg_freight_cost DESC
""")
save_mart(mart_customer_satisfaction, "mart_customer_satisfaction")

# -------------------------------------------------------------------
# MART 8: ORDER FUNNEL - answers Q5
# -------------------------------------------------------------------
print("\n8️⃣ Building mart_order_funnel...", flush=True)
mart_order_funnel = spark.sql("""
    SELECT
        d.year_number,
        d.quarter_number,
        d.month_number,
        d.month_name,
        COUNT(DISTINCT fo.order_id) AS ordered_orders,
        SUM(CASE WHEN fo.order_approved_at IS NOT NULL THEN 1 ELSE 0 END) AS approved_orders,
        SUM(CASE WHEN fo.order_delivered_carrier_date IS NOT NULL THEN 1 ELSE 0 END) AS shipped_orders,
        SUM(CASE WHEN fo.order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END) AS delivered_orders,
        COUNT(DISTINCT rbo.order_id) AS reviewed_orders,
        ROUND(100.0 * SUM(CASE WHEN fo.order_approved_at IS NOT NULL THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS approval_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN fo.order_delivered_carrier_date IS NOT NULL THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS shipping_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN fo.order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END) / COUNT(DISTINCT fo.order_id), 2) AS delivery_rate_pct,
        ROUND(100.0 * COUNT(DISTINCT rbo.order_id) / COUNT(DISTINCT fo.order_id), 2) AS review_rate_pct,
        ROUND(AVG(DATEDIFF(fo.order_approved_at, fo.order_purchase_timestamp)), 2) AS avg_days_to_approve,
        ROUND(AVG(DATEDIFF(fo.order_delivered_carrier_date, fo.order_approved_at)), 2) AS avg_days_approved_to_shipped,
        ROUND(AVG(DATEDIFF(fo.order_delivered_customer_date, fo.order_delivered_carrier_date)), 2) AS avg_days_shipped_to_delivered
    FROM fct_orders fo
    LEFT JOIN dim_date d ON fo.purchase_date_sk_fk = d.date_sk
    LEFT JOIN review_by_order rbo ON fo.order_id = rbo.order_id
    GROUP BY d.year_number, d.quarter_number, d.month_number, d.month_name
    ORDER BY d.year_number, d.month_number
""")
save_mart(mart_order_funnel, "mart_order_funnel")

# -------------------------------------------------------------------
# MART 9: PAYMENT ANALYTICS - extra dashboard insights
# -------------------------------------------------------------------
print("\n9️⃣ Building mart_payment_analytics...", flush=True)
mart_payment_analytics = spark.sql("""
    SELECT
        d.year_number,
        d.quarter_number,
        d.month_number,
        d.month_name,
        p.payment_type,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT p.order_id) AS total_orders,
        ROUND(SUM(p.payment_value), 2) AS total_payment_value,
        ROUND(AVG(p.payment_value), 2) AS avg_payment_value,
        ROUND(AVG(p.payment_installments), 2) AS avg_installments,
        SUM(CASE WHEN p.is_installment_payment = 1 THEN 1 ELSE 0 END) AS installment_transactions,
        ROUND(100.0 * SUM(CASE WHEN p.is_installment_payment = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS installment_rate_pct
    FROM fct_customer_payment p
    LEFT JOIN fct_orders fo ON p.order_id = fo.order_id
    LEFT JOIN dim_date d ON fo.purchase_date_sk_fk = d.date_sk
    WHERE p.payment_type IS NOT NULL
    GROUP BY d.year_number, d.quarter_number, d.month_number, d.month_name, p.payment_type
    ORDER BY d.year_number, d.month_number, total_payment_value DESC
""")
save_mart(mart_payment_analytics, "mart_payment_analytics")

# ===================================================================
# 6. FINAL SUMMARY
# ===================================================================
print("\n" + "=" * 70)
print("🎉 ALL DATA MARTS CREATED SUCCESSFULLY!")
print("=" * 70)
print("Created tables:")
print("   1. mart_sales_analytics")
print("   2. mart_seller_performance")
print("   3. mart_seller_alerts")
print("   4. mart_product_analytics")
print("   5. mart_delivery_analytics")
print("   6. mart_customer_analytics")
print("   7. mart_customer_satisfaction")
print("   8. mart_order_funnel")
print("   9. mart_payment_analytics")
print("\n✅ Ready for Power BI!")
print("=" * 70)

spark.stop()
print("\n✨ Spark session stopped successfully!", flush=True)
