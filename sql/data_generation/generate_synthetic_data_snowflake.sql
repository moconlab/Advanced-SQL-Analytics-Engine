-- Synthetic Sales and Events Data Generation for Snowflake
-- This script generates a large synthetic dataset for advanced SQL analytics
-- Run this in your Snowflake environment to create the base tables

-- ============================================================================
-- 1. CREATE USERS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE raw_users AS
WITH user_base AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS user_id,
        UNIFORM(18, 75, RANDOM()) AS age,
        CASE UNIFORM(0, 3, RANDOM())
            WHEN 0 THEN 'North America'
            WHEN 1 THEN 'Europe'
            WHEN 2 THEN 'Asia'
            ELSE 'Other'
        END AS region,
        CASE UNIFORM(0, 4, RANDOM())
            WHEN 0 THEN 'Mobile'
            WHEN 1 THEN 'Desktop'
            WHEN 2 THEN 'Tablet'
            ELSE 'Mobile'
        END AS device_type,
        DATEADD(
            DAY, 
            -UNIFORM(0, 730, RANDOM()), 
            '2024-12-31'::DATE
        ) AS signup_date
    FROM TABLE(GENERATOR(ROWCOUNT => 50000))
)
SELECT 
    user_id,
    'user_' || LPAD(user_id::VARCHAR, 8, '0') AS user_email,
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
    DATEADD(MONTH, CAST(signup_date AS DATE), CURRENT_DATE()) AS cohort_month
FROM user_base;

-- ============================================================================
-- 2. CREATE PRODUCTS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE raw_products AS
WITH product_categories AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS product_id,
        CASE UNIFORM(0, 5, RANDOM())
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Clothing'
            WHEN 2 THEN 'Home & Garden'
            WHEN 3 THEN 'Books'
            ELSE 'Sports'
        END AS category,
        UNIFORM(5, 500, RANDOM()) AS base_price
    FROM TABLE(GENERATOR(ROWCOUNT => 1000))
)
SELECT 
    product_id,
    'Product_' || product_id AS product_name,
    category,
    base_price,
    base_price * UNIFORM(0.7, 1.3, RANDOM()) AS current_price,
    CASE UNIFORM(0, 2, RANDOM())
        WHEN 0 THEN 'Brand A'
        ELSE 'Brand B'
    END AS brand
FROM product_categories;

-- ============================================================================
-- 3. CREATE EVENTS TABLE (User Activity Stream)
-- ============================================================================
CREATE OR REPLACE TABLE raw_events AS
WITH event_stream AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS event_id,
        UNIFORM(1, 50000, RANDOM()) AS user_id,
        UNIFORM(1, 1000, RANDOM()) AS product_id,
        CASE UNIFORM(0, 4, RANDOM())
            WHEN 0 THEN 'page_view'
            WHEN 1 THEN 'product_view'
            WHEN 2 THEN 'add_to_cart'
            WHEN 3 THEN 'remove_from_cart'
            ELSE 'page_view'
        END AS event_type,
        DATEADD(
            SECOND,
            -UNIFORM(0, 63072000, RANDOM()),  -- Up to 2 years of data
            '2024-12-31 23:59:59'::TIMESTAMP
        ) AS event_timestamp,
        UNIFORM(1, 300, RANDOM()) AS session_duration_seconds
    FROM TABLE(GENERATOR(ROWCOUNT => 5000000))  -- 5M events
)
SELECT 
    event_id,
    user_id,
    product_id,
    event_type,
    event_timestamp,
    DATE(event_timestamp) AS event_date,
    session_duration_seconds,
    CASE UNIFORM(0, 3, RANDOM())
        WHEN 0 THEN 'organic'
        WHEN 1 THEN 'paid_search'
        WHEN 2 THEN 'social'
        ELSE 'direct'
    END AS traffic_source,
    OBJECT_CONSTRUCT(
        'page_url', '/product/' || product_id,
        'referrer', 'https://example.com',
        'user_agent', 'Mozilla/5.0'
    ) AS event_properties
FROM event_stream;

-- ============================================================================
-- 4. CREATE SALES TABLE (Completed Purchases)
-- ============================================================================
CREATE OR REPLACE TABLE raw_sales AS
WITH purchase_events AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS sale_id,
        UNIFORM(1, 50000, RANDOM()) AS user_id,
        UNIFORM(1, 1000, RANDOM()) AS product_id,
        DATEADD(
            SECOND,
            -UNIFORM(0, 63072000, RANDOM()),
            '2024-12-31 23:59:59'::TIMESTAMP
        ) AS purchase_timestamp,
        UNIFORM(1, 5, RANDOM()) AS quantity
    FROM TABLE(GENERATOR(ROWCOUNT => 500000))  -- 500K sales
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
    (s.quantity * p.current_price) * UNIFORM(0.05, 0.15, RANDOM()) AS discount_amount,
    (s.quantity * p.current_price) * (1 - UNIFORM(0.05, 0.15, RANDOM())) AS net_amount,
    CASE UNIFORM(0, 3, RANDOM())
        WHEN 0 THEN 'credit_card'
        WHEN 1 THEN 'paypal'
        WHEN 2 THEN 'bank_transfer'
        ELSE 'credit_card'
    END AS payment_method,
    CASE UNIFORM(0, 10, RANDOM())
        WHEN 0 THEN 'completed'
        WHEN 1 THEN 'refunded'
        ELSE 'completed'
    END AS order_status
FROM purchase_events s
JOIN raw_products p ON s.product_id = p.product_id;

-- ============================================================================
-- 5. CREATE INDEXES FOR PERFORMANCE
-- ============================================================================

-- Create clustering keys for better performance in Snowflake
ALTER TABLE raw_events CLUSTER BY (event_date, user_id);
ALTER TABLE raw_sales CLUSTER BY (purchase_date, user_id);

-- ============================================================================
-- 6. VERIFY DATA GENERATION
-- ============================================================================

SELECT 'Users' AS table_name, COUNT(*) AS row_count FROM raw_users
UNION ALL
SELECT 'Products', COUNT(*) FROM raw_products
UNION ALL
SELECT 'Events', COUNT(*) FROM raw_events
UNION ALL
SELECT 'Sales', COUNT(*) FROM raw_sales;

-- Show date ranges
SELECT 
    'Events' AS table_name,
    MIN(event_date) AS min_date,
    MAX(event_date) AS max_date,
    COUNT(DISTINCT user_id) AS unique_users
FROM raw_events
UNION ALL
SELECT 
    'Sales',
    MIN(purchase_date),
    MAX(purchase_date),
    COUNT(DISTINCT user_id)
FROM raw_sales;
