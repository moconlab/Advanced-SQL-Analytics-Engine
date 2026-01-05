-- Synthetic Sales and Events Data Generation for BigQuery
-- This script generates a large synthetic dataset for advanced SQL analytics
-- Run this in your BigQuery environment to create the base tables

-- ============================================================================
-- 1. CREATE USERS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.raw_users` AS
WITH user_base AS (
    SELECT 
        user_id,
        CAST(FLOOR(18 + RAND() * 57) AS INT64) AS age,
        CASE CAST(FLOOR(RAND() * 4) AS INT64)
            WHEN 0 THEN 'North America'
            WHEN 1 THEN 'Europe'
            WHEN 2 THEN 'Asia'
            ELSE 'Other'
        END AS region,
        CASE CAST(FLOOR(RAND() * 4) AS INT64)
            WHEN 0 THEN 'Mobile'
            WHEN 1 THEN 'Desktop'
            WHEN 2 THEN 'Tablet'
            ELSE 'Mobile'
        END AS device_type,
        DATE_SUB('2024-12-31', INTERVAL CAST(FLOOR(RAND() * 730) AS INT64) DAY) AS signup_date
    FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS user_id
)
SELECT 
    user_id,
    CONCAT('user_', LPAD(CAST(user_id AS STRING), 8, '0')) AS user_email,
    age,
    CASE 
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END AS age_group,
    region,
    device_type,
    signup_date,
    DATE_TRUNC(signup_date, MONTH) AS cohort_month
FROM user_base;

-- ============================================================================
-- 2. CREATE PRODUCTS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.raw_products` AS
WITH product_categories AS (
    SELECT 
        product_id,
        CASE CAST(FLOOR(RAND() * 5) AS INT64)
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Clothing'
            WHEN 2 THEN 'Home & Garden'
            WHEN 3 THEN 'Books'
            ELSE 'Sports'
        END AS category,
        5 + CAST(FLOOR(RAND() * 495) AS INT64) AS base_price
    FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS product_id
)
SELECT 
    product_id,
    CONCAT('Product_', CAST(product_id AS STRING)) AS product_name,
    category,
    base_price,
    base_price * (0.7 + RAND() * 0.6) AS current_price,
    CASE CAST(FLOOR(RAND() * 2) AS INT64)
        WHEN 0 THEN 'Brand A'
        ELSE 'Brand B'
    END AS brand
FROM product_categories;

-- ============================================================================
-- 3. CREATE EVENTS TABLE (User Activity Stream)
-- ============================================================================
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.raw_events` AS
WITH event_stream AS (
    SELECT 
        event_id,
        CAST(FLOOR(1 + RAND() * 50000) AS INT64) AS user_id,
        CAST(FLOOR(1 + RAND() * 1000) AS INT64) AS product_id,
        CASE CAST(FLOOR(RAND() * 5) AS INT64)
            WHEN 0 THEN 'page_view'
            WHEN 1 THEN 'product_view'
            WHEN 2 THEN 'add_to_cart'
            WHEN 3 THEN 'remove_from_cart'
            ELSE 'page_view'
        END AS event_type,
        TIMESTAMP_SUB(
            TIMESTAMP('2024-12-31 23:59:59'),
            INTERVAL CAST(FLOOR(RAND() * 63072000) AS INT64) SECOND
        ) AS event_timestamp,
        CAST(FLOOR(1 + RAND() * 300) AS INT64) AS session_duration_seconds
    FROM UNNEST(GENERATE_ARRAY(1, 5000000)) AS event_id
)
SELECT 
    event_id,
    user_id,
    product_id,
    event_type,
    event_timestamp,
    DATE(event_timestamp) AS event_date,
    session_duration_seconds,
    CASE CAST(FLOOR(RAND() * 4) AS INT64)
        WHEN 0 THEN 'organic'
        WHEN 1 THEN 'paid_search'
        WHEN 2 THEN 'social'
        ELSE 'direct'
    END AS traffic_source,
    STRUCT(
        CONCAT('/product/', CAST(product_id AS STRING)) AS page_url,
        'https://example.com' AS referrer,
        'Mozilla/5.0' AS user_agent
    ) AS event_properties
FROM event_stream;

-- ============================================================================
-- 4. CREATE SALES TABLE (Completed Purchases)
-- ============================================================================
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.raw_sales` AS
WITH purchase_events AS (
    SELECT 
        sale_id,
        CAST(FLOOR(1 + RAND() * 50000) AS INT64) AS user_id,
        CAST(FLOOR(1 + RAND() * 1000) AS INT64) AS product_id,
        TIMESTAMP_SUB(
            TIMESTAMP('2024-12-31 23:59:59'),
            INTERVAL CAST(FLOOR(RAND() * 63072000) AS INT64) SECOND
        ) AS purchase_timestamp,
        CAST(FLOOR(1 + RAND() * 5) AS INT64) AS quantity
    FROM UNNEST(GENERATE_ARRAY(1, 500000)) AS sale_id
)
SELECT 
    s.sale_id,
    s.user_id,
    s.product_id,
    s.purchase_timestamp,
    DATE(s.purchase_timestamp) AS purchase_date,
    s.quantity,
    p.current_price,
    s.quantity * p.current_price AS total_amount,
    (s.quantity * p.current_price) * (0.05 + RAND() * 0.1) AS discount_amount,
    (s.quantity * p.current_price) * (1 - (0.05 + RAND() * 0.1)) AS net_amount,
    CASE CAST(FLOOR(RAND() * 3) AS INT64)
        WHEN 0 THEN 'credit_card'
        WHEN 1 THEN 'paypal'
        WHEN 2 THEN 'bank_transfer'
        ELSE 'credit_card'
    END AS payment_method,
    CASE CAST(FLOOR(RAND() * 10) AS INT64)
        WHEN 0 THEN 'completed'
        WHEN 1 THEN 'refunded'
        ELSE 'completed'
    END AS order_status
FROM purchase_events s
JOIN `{{ project_id }}.analytics.raw_products` p ON s.product_id = p.product_id;

-- ============================================================================
-- 5. CREATE PARTITIONING AND CLUSTERING FOR PERFORMANCE
-- ============================================================================

-- Recreate events table with partitioning and clustering
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.raw_events`
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_type
AS SELECT * FROM `{{ project_id }}.analytics.raw_events`;

-- Recreate sales table with partitioning and clustering  
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.raw_sales`
PARTITION BY DATE(purchase_timestamp)
CLUSTER BY user_id, product_id
AS SELECT * FROM `{{ project_id }}.analytics.raw_sales`;

-- ============================================================================
-- 6. VERIFY DATA GENERATION
-- ============================================================================

SELECT 'Users' AS table_name, COUNT(*) AS row_count 
FROM `{{ project_id }}.analytics.raw_users`
UNION ALL
SELECT 'Products', COUNT(*) 
FROM `{{ project_id }}.analytics.raw_products`
UNION ALL
SELECT 'Events', COUNT(*) 
FROM `{{ project_id }}.analytics.raw_events`
UNION ALL
SELECT 'Sales', COUNT(*) 
FROM `{{ project_id }}.analytics.raw_sales`;

-- Show date ranges
SELECT 
    'Events' AS table_name,
    MIN(event_date) AS min_date,
    MAX(event_date) AS max_date,
    COUNT(DISTINCT user_id) AS unique_users
FROM `{{ project_id }}.analytics.raw_events`
UNION ALL
SELECT 
    'Sales',
    MIN(purchase_date),
    MAX(purchase_date),
    COUNT(DISTINCT user_id)
FROM `{{ project_id }}.analytics.raw_sales`;
