-- Advanced Window Functions Examples
-- This file demonstrates various window function patterns for analytics

-- ============================================================================
-- 1. RUNNING TOTALS AND CUMULATIVE METRICS
-- ============================================================================

-- Running total of revenue by user
SELECT 
    user_id,
    purchase_date,
    net_amount,
    SUM(net_amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM raw_sales
ORDER BY user_id, purchase_date;

-- Cumulative count of purchases
SELECT 
    purchase_date,
    COUNT(*) AS daily_purchases,
    SUM(COUNT(*)) OVER (
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_purchases
FROM raw_sales
GROUP BY purchase_date
ORDER BY purchase_date;

-- ============================================================================
-- 2. MOVING AVERAGES AND ROLLING METRICS
-- ============================================================================

-- 7-day moving average of daily revenue
SELECT 
    purchase_date,
    SUM(net_amount) AS daily_revenue,
    AVG(SUM(net_amount)) OVER (
        ORDER BY purchase_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day,
    AVG(SUM(net_amount)) OVER (
        ORDER BY purchase_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30day
FROM raw_sales
GROUP BY purchase_date
ORDER BY purchase_date;

-- Rolling standard deviation
SELECT 
    purchase_date,
    AVG(net_amount) AS avg_order_value,
    STDDEV(net_amount) OVER (
        ORDER BY purchase_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_stddev_30day
FROM raw_sales
GROUP BY purchase_date, net_amount
ORDER BY purchase_date;

-- ============================================================================
-- 3. RANKINGS AND PERCENTILES
-- ============================================================================

-- Top products by revenue with rankings
SELECT 
    product_id,
    SUM(net_amount) AS total_revenue,
    ROW_NUMBER() OVER (ORDER BY SUM(net_amount) DESC) AS row_num,
    RANK() OVER (ORDER BY SUM(net_amount) DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY SUM(net_amount) DESC) AS dense_rank,
    PERCENT_RANK() OVER (ORDER BY SUM(net_amount) DESC) AS percent_rank
FROM raw_sales
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 20;

-- Quartile segmentation
SELECT 
    user_id,
    SUM(net_amount) AS total_spent,
    NTILE(4) OVER (ORDER BY SUM(net_amount) DESC) AS spending_quartile,
    CASE NTILE(4) OVER (ORDER BY SUM(net_amount) DESC)
        WHEN 1 THEN 'VIP'
        WHEN 2 THEN 'High Value'
        WHEN 3 THEN 'Medium Value'
        WHEN 4 THEN 'Low Value'
    END AS customer_segment
FROM raw_sales
GROUP BY user_id
ORDER BY total_spent DESC;

-- ============================================================================
-- 4. LAG AND LEAD FOR TIME-BASED ANALYSIS
-- ============================================================================

-- Time between purchases for each user
SELECT 
    user_id,
    purchase_date,
    LAG(purchase_date) OVER (PARTITION BY user_id ORDER BY purchase_date) AS prev_purchase,
    LEAD(purchase_date) OVER (PARTITION BY user_id ORDER BY purchase_date) AS next_purchase,
    DATEDIFF(
        day,
        LAG(purchase_date) OVER (PARTITION BY user_id ORDER BY purchase_date),
        purchase_date
    ) AS days_since_last_purchase
FROM raw_sales
ORDER BY user_id, purchase_date;

-- Compare current vs previous period
SELECT 
    purchase_date,
    SUM(net_amount) AS current_revenue,
    LAG(SUM(net_amount)) OVER (ORDER BY purchase_date) AS prev_day_revenue,
    SUM(net_amount) - LAG(SUM(net_amount)) OVER (ORDER BY purchase_date) AS revenue_change,
    ROUND(
        100.0 * (SUM(net_amount) - LAG(SUM(net_amount)) OVER (ORDER BY purchase_date)) / 
        NULLIF(LAG(SUM(net_amount)) OVER (ORDER BY purchase_date), 0),
        2
    ) AS revenue_change_pct
FROM raw_sales
GROUP BY purchase_date
ORDER BY purchase_date;

-- ============================================================================
-- 5. FIRST_VALUE AND LAST_VALUE
-- ============================================================================

-- Compare each purchase to user's first and last purchase
SELECT 
    user_id,
    purchase_date,
    net_amount,
    FIRST_VALUE(net_amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_purchase_amount,
    LAST_VALUE(net_amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_purchase_amount
FROM raw_sales
ORDER BY user_id, purchase_date;

-- ============================================================================
-- 6. COMPLEX WINDOW FUNCTION COMBINATIONS
-- ============================================================================

-- Customer lifecycle analysis
WITH customer_metrics AS (
    SELECT 
        user_id,
        purchase_date,
        net_amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date) AS purchase_number,
        COUNT(*) OVER (PARTITION BY user_id) AS total_purchases,
        SUM(net_amount) OVER (
            PARTITION BY user_id 
            ORDER BY purchase_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_spent,
        AVG(net_amount) OVER (
            PARTITION BY user_id
            ORDER BY purchase_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3_purchases
    FROM raw_sales
)
SELECT 
    user_id,
    purchase_date,
    purchase_number,
    total_purchases,
    net_amount,
    cumulative_spent,
    moving_avg_3_purchases,
    CASE 
        WHEN purchase_number = 1 THEN 'First Purchase'
        WHEN purchase_number = total_purchases THEN 'Latest Purchase'
        ELSE 'Repeat Purchase'
    END AS purchase_type
FROM customer_metrics
ORDER BY user_id, purchase_date;
