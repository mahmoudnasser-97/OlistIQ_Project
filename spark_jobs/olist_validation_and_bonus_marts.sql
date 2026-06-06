/*
===============================================================================
 Olist DW Validation + Bonus Data Marts SQL Script
 Purpose:
   1) Validate Gold layer fact/dimension record counts.
   2) Validate relationship integrity between facts and dimensions.
   3) Validate serving-layer marts record counts.
   4) Recreate optional bonus marts that were dropped:
      - mart_geo_heatmap
      - mart_review_sentiment

 Run in PgAdmin Query Tool connected to database: olist_dw
 Schema: public
===============================================================================
*/

-- =============================================================================
-- SECTION 1: GOLD TABLE RECORD COUNTS
-- This query checks that all final Gold dimension and fact tables contain records.
-- If any count = 0, that table did not load correctly.
-- =============================================================================
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_order_status_detail', COUNT(*) FROM dim_order_status_detail
UNION ALL SELECT 'fct_orders', COUNT(*) FROM fct_orders
UNION ALL SELECT 'fct_seller_fulfillment', COUNT(*) FROM fct_seller_fulfillment
UNION ALL SELECT 'fct_customer_payment', COUNT(*) FROM fct_customer_payment
UNION ALL SELECT 'fct_customer_review', COUNT(*) FROM fct_customer_review
ORDER BY table_name;

-- =============================================================================
-- SECTION 2: MART TABLE RECORD COUNTS
-- This query checks that all new serving-layer marts contain records.
-- Note: mart_seller_alerts may be empty if no seller matches late rate > 10% in last 90 days.
-- =============================================================================
SELECT 'mart_sales_analytics' AS mart_name, COUNT(*) AS row_count FROM mart_sales_analytics
UNION ALL SELECT 'mart_seller_performance', COUNT(*) FROM mart_seller_performance
UNION ALL SELECT 'mart_product_analytics', COUNT(*) FROM mart_product_analytics
UNION ALL SELECT 'mart_delivery_analytics', COUNT(*) FROM mart_delivery_analytics
UNION ALL SELECT 'mart_customer_analytics', COUNT(*) FROM mart_customer_analytics
UNION ALL SELECT 'mart_customer_satisfaction', COUNT(*) FROM mart_customer_satisfaction
UNION ALL SELECT 'mart_order_funnel', COUNT(*) FROM mart_order_funnel
UNION ALL SELECT 'mart_payment_analytics', COUNT(*) FROM mart_payment_analytics
ORDER BY mart_name;

-- Optional: run this separately only if mart_seller_alerts exists.
-- SELECT 'mart_seller_alerts' AS mart_name, COUNT(*) AS row_count FROM mart_seller_alerts;

-- =============================================================================
-- SECTION 3: LIST ALL MART TABLES
-- This helps you confirm which mart tables currently exist in PostgreSQL.
-- =============================================================================
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'mart_%'
ORDER BY table_name;

-- =============================================================================
-- SECTION 4: RELATIONSHIP VALIDATION - fct_orders to dim_customer
-- Expected result: missing_customers = 0
-- If result > 0, some orders have customer foreign keys not found in dim_customer.
-- =============================================================================
SELECT COUNT(*) AS missing_customers
FROM fct_orders fo
LEFT JOIN dim_customer c
  ON fo.customer_sk_fk = c.customer_sk
WHERE c.customer_sk IS NULL;

-- =============================================================================
-- SECTION 5: RELATIONSHIP VALIDATION - fct_orders to dim_date
-- Expected result: missing_purchase_dates = 0
-- This validates the purchase date role of dim_date.
-- =============================================================================
SELECT COUNT(*) AS missing_purchase_dates
FROM fct_orders fo
LEFT JOIN dim_date d
  ON fo.purchase_date_sk_fk = d.date_sk
WHERE d.date_sk IS NULL;

-- =============================================================================
-- SECTION 6: RELATIONSHIP VALIDATION - fct_orders to dim_order_status_detail
-- Expected result: missing_statuses = 0
-- This validates order status/detail foreign keys.
-- =============================================================================
SELECT COUNT(*) AS missing_statuses
FROM fct_orders fo
LEFT JOIN dim_order_status_detail st
  ON fo.status_sk_fk = st.status_sk
WHERE st.status_sk IS NULL;

-- =============================================================================
-- SECTION 7: RELATIONSHIP VALIDATION - fct_seller_fulfillment
-- Expected result: all missing columns = 0
-- This checks seller, product, customer, and date dimension links.
-- =============================================================================
SELECT
  SUM(CASE WHEN s.seller_sk IS NULL THEN 1 ELSE 0 END) AS missing_sellers,
  SUM(CASE WHEN p.product_sk IS NULL THEN 1 ELSE 0 END) AS missing_products,
  SUM(CASE WHEN c.customer_sk IS NULL THEN 1 ELSE 0 END) AS missing_customers,
  SUM(CASE WHEN d.date_sk IS NULL THEN 1 ELSE 0 END) AS missing_purchase_dates
FROM fct_seller_fulfillment sf
LEFT JOIN dim_seller s ON sf.seller_sk_fk = s.seller_sk
LEFT JOIN dim_product p ON sf.product_sk_fk = p.product_sk
LEFT JOIN dim_customer c ON sf.customer_sk_fk = c.customer_sk
LEFT JOIN dim_date d ON sf.purchase_date_sk_fk = d.date_sk;

-- =============================================================================
-- SECTION 8: RELATIONSHIP VALIDATION - Reviews to Orders
-- Expected result: reviews_without_order = 0
-- This validates that every review belongs to an existing order.
-- =============================================================================
SELECT COUNT(*) AS reviews_without_order
FROM fct_customer_review r
LEFT JOIN fct_orders o
  ON r.order_id = o.order_id
WHERE o.order_id IS NULL;

-- =============================================================================
-- SECTION 9: RELATIONSHIP VALIDATION - Payments to Orders
-- Expected result: payments_without_order = 0
-- This validates that every payment belongs to an existing order.
-- =============================================================================
SELECT COUNT(*) AS payments_without_order
FROM fct_customer_payment p
LEFT JOIN fct_orders o
  ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- =============================================================================
-- SECTION 10: DATA QUALITY CHECK - Null business keys in facts
-- These checks help catch broken fact records.
-- =============================================================================
SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
  SUM(CASE WHEN customer_sk_fk IS NULL THEN 1 ELSE 0 END) AS null_customer_fk,
  SUM(CASE WHEN purchase_date_sk_fk IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_fk
FROM fct_orders;

SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
  SUM(CASE WHEN seller_sk_fk IS NULL THEN 1 ELSE 0 END) AS null_seller_fk,
  SUM(CASE WHEN product_sk_fk IS NULL THEN 1 ELSE 0 END) AS null_product_fk,
  SUM(CASE WHEN customer_sk_fk IS NULL THEN 1 ELSE 0 END) AS null_customer_fk
FROM fct_seller_fulfillment;

-- =============================================================================
-- SECTION 11: SAMPLE DATA FROM EACH MART
-- Use these queries to quickly inspect mart content in PgAdmin.
-- =============================================================================
SELECT * FROM mart_sales_analytics LIMIT 20;
SELECT * FROM mart_seller_performance LIMIT 20;
SELECT * FROM mart_product_analytics LIMIT 20;
SELECT * FROM mart_delivery_analytics LIMIT 20;
SELECT * FROM mart_customer_analytics LIMIT 20;
SELECT * FROM mart_customer_satisfaction LIMIT 20;
SELECT * FROM mart_order_funnel LIMIT 20;
SELECT * FROM mart_payment_analytics LIMIT 20;
-- SELECT * FROM mart_seller_alerts LIMIT 20;

-- =============================================================================
-- SECTION 12: BONUS MART 1 - Geo Heatmap
-- Recreates the dropped mart_geo_heatmap.
-- Purpose in Power BI:
--   - Map orders, revenue, customers, AOV, and delivery performance by state/region.
--   - Very useful for map visual, filled map, or regional performance heatmap.
-- =============================================================================
DROP TABLE IF EXISTS mart_geo_heatmap CASCADE;

CREATE TABLE mart_geo_heatmap AS
SELECT
    c.customer_state,
    c.customer_region,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(COALESCE(o.total_order_cost, 0))::numeric, 2) AS total_revenue,
    COUNT(DISTINCT o.customer_sk_fk) AS total_customers,
    ROUND(AVG(COALESCE(o.total_order_cost, 0))::numeric, 2) AS avg_order_value,
    SUM(CASE WHEN o.delivery_status_detail = 'Delivered on time' THEN 1 ELSE 0 END) AS on_time_orders,
    SUM(CASE WHEN o.delivery_status_detail = 'Delayed' THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(
      100.0 * SUM(CASE WHEN o.delivery_status_detail = 'Delivered on time' THEN 1 ELSE 0 END)
      / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
    ) AS on_time_rate_pct,
    ROUND(AVG(o.total_lead_time)::numeric, 2) AS avg_lead_time_days
FROM fct_orders o
JOIN dim_customer c
  ON o.customer_sk_fk = c.customer_sk
WHERE c.customer_state IS NOT NULL
GROUP BY c.customer_state, c.customer_region
ORDER BY total_revenue DESC;

-- =============================================================================
-- SECTION 13: BONUS MART 2 - Review Sentiment
-- Recreates the dropped mart_review_sentiment.
-- Purpose in Power BI:
--   - Analyze satisfaction by product category and customer region.
--   - Useful for category sentiment visuals and negative review diagnosis.
-- Notes:
--   - This version supports review_label values like Satisfied / Neutral / Unsatisfied.
--   - It also uses review_score logic as a fallback.
-- =============================================================================
DROP TABLE IF EXISTS mart_review_sentiment CASCADE;

CREATE TABLE mart_review_sentiment AS
WITH review_product_base AS (
    SELECT DISTINCT
        r.review_id,
        r.order_id,
        r.review_score,
        r.review_label,
        r.review_response_delay_days,
        c.customer_region,
        c.customer_state,
        p.product_category_name
    FROM fct_customer_review r
    JOIN dim_customer c
      ON r.customer_sk_fk = c.customer_sk
    JOIN fct_seller_fulfillment sf
      ON r.order_id = sf.order_id
    JOIN dim_product p
      ON sf.product_sk_fk = p.product_sk
    WHERE p.product_category_name IS NOT NULL
      AND c.customer_region IS NOT NULL
)
SELECT
    product_category_name,
    customer_region,
    COUNT(DISTINCT review_id) AS total_reviews,
    ROUND(AVG(review_score)::numeric, 2) AS avg_score,
    SUM(CASE WHEN review_label IN ('Satisfied', 'Positive') OR review_score >= 4 THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN review_label = 'Neutral' OR review_score = 3 THEN 1 ELSE 0 END) AS neutral_count,
    SUM(CASE WHEN review_label IN ('Unsatisfied', 'Negative') OR review_score <= 2 THEN 1 ELSE 0 END) AS negative_count,
    ROUND(AVG(review_response_delay_days)::numeric, 2) AS avg_response_delay_days,
    ROUND(
      100.0 * SUM(CASE WHEN review_label IN ('Satisfied', 'Positive') OR review_score >= 4 THEN 1 ELSE 0 END)
      / NULLIF(COUNT(DISTINCT review_id), 0), 2
    ) AS pct_positive,
    ROUND(
      100.0 * SUM(CASE WHEN review_label IN ('Unsatisfied', 'Negative') OR review_score <= 2 THEN 1 ELSE 0 END)
      / NULLIF(COUNT(DISTINCT review_id), 0), 2
    ) AS pct_negative
FROM review_product_base
GROUP BY product_category_name, customer_region
ORDER BY total_reviews DESC;

-- =============================================================================
-- SECTION 14: CHECK BONUS MARTS AFTER CREATION
-- Confirms bonus marts are created and populated.
-- =============================================================================
SELECT 'mart_geo_heatmap' AS mart_name, COUNT(*) AS row_count FROM mart_geo_heatmap
UNION ALL SELECT 'mart_review_sentiment', COUNT(*) FROM mart_review_sentiment;

SELECT * FROM mart_geo_heatmap LIMIT 20;
SELECT * FROM mart_review_sentiment LIMIT 20;
