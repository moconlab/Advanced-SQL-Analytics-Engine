-- Funnel Metrics Query Examples
-- Analyzing conversion funnels and drop-off points

-- ============================================================================
-- 1. BASIC FUNNEL OVERVIEW
-- ============================================================================

-- Daily funnel performance
SELECT 
    event_date,
    total_users,
    users_page_view,
    users_product_view,
    users_add_to_cart,
    users_purchase,
    conversion_page_to_product_pct,
    conversion_product_to_cart_pct,
    conversion_cart_to_purchase_pct,
    conversion_overall_pct
FROM {{ ref('funnel_metrics') }}
WHERE event_date >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY event_date DESC;

-- ============================================================================
-- 2. FUNNEL VISUALIZATION DATA
-- ============================================================================

-- Get funnel shape for visualization
WITH funnel_totals AS (
    SELECT 
        SUM(users_page_view) AS stage_1_page_view,
        SUM(users_product_view) AS stage_2_product_view,
        SUM(users_add_to_cart) AS stage_3_add_to_cart,
        SUM(users_purchase) AS stage_4_purchase
    FROM {{ ref('funnel_metrics') }}
    WHERE event_date >= DATEADD(day, -30, CURRENT_DATE())
)

SELECT 
    'Page View' AS stage,
    1 AS stage_order,
    stage_1_page_view AS users,
    100.0 AS pct_of_start,
    0.0 AS drop_off_pct
FROM funnel_totals
UNION ALL
SELECT 
    'Product View',
    2,
    stage_2_product_view,
    ROUND(100.0 * stage_2_product_view / stage_1_page_view, 2),
    ROUND(100.0 * (stage_1_page_view - stage_2_product_view) / stage_1_page_view, 2)
FROM funnel_totals
UNION ALL
SELECT 
    'Add to Cart',
    3,
    stage_3_add_to_cart,
    ROUND(100.0 * stage_3_add_to_cart / stage_1_page_view, 2),
    ROUND(100.0 * (stage_2_product_view - stage_3_add_to_cart) / stage_2_product_view, 2)
FROM funnel_totals
UNION ALL
SELECT 
    'Purchase',
    4,
    stage_4_purchase,
    ROUND(100.0 * stage_4_purchase / stage_1_page_view, 2),
    ROUND(100.0 * (stage_3_add_to_cart - stage_4_purchase) / stage_3_add_to_cart, 2)
FROM funnel_totals
ORDER BY stage_order;

-- ============================================================================
-- 3. FUNNEL BY SEGMENTS
-- ============================================================================

-- Compare funnel performance by device type
SELECT 
    device_type,
    SUM(users_page_view) AS total_page_views,
    SUM(users_purchase) AS total_purchases,
    ROUND(AVG(conversion_overall_pct), 2) AS avg_conversion_rate,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    SUM(total_revenue) AS total_revenue
FROM {{ ref('funnel_metrics') }}
WHERE event_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY device_type
ORDER BY total_revenue DESC;

-- Compare by region
SELECT 
    region,
    SUM(users_page_view) AS total_page_views,
    SUM(users_purchase) AS total_purchases,
    ROUND(AVG(conversion_overall_pct), 2) AS avg_conversion_rate,
    ROUND(AVG(dropoff_add_to_cart_pct), 2) AS avg_cart_abandonment_rate,
    SUM(total_revenue) AS total_revenue
FROM {{ ref('funnel_metrics') }}
WHERE event_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY region
ORDER BY avg_conversion_rate DESC;

-- ============================================================================
-- 4. IDENTIFY PROBLEM AREAS
-- ============================================================================

-- Find dates with unusually high drop-off rates
WITH avg_metrics AS (
    SELECT 
        AVG(dropoff_page_view_pct) AS avg_page_dropoff,
        AVG(dropoff_product_view_pct) AS avg_product_dropoff,
        AVG(dropoff_add_to_cart_pct) AS avg_cart_dropoff,
        STDDEV(dropoff_page_view_pct) AS std_page_dropoff,
        STDDEV(dropoff_product_view_pct) AS std_product_dropoff,
        STDDEV(dropoff_add_to_cart_pct) AS std_cart_dropoff
    FROM {{ ref('funnel_metrics') }}
    WHERE event_date >= DATEADD(day, -90, CURRENT_DATE())
)

SELECT 
    f.event_date,
    f.device_type,
    f.dropoff_page_view_pct,
    f.dropoff_product_view_pct,
    f.dropoff_add_to_cart_pct,
    CASE 
        WHEN f.dropoff_page_view_pct > a.avg_page_dropoff + (2 * a.std_page_dropoff) THEN 'High Page Drop-off'
        WHEN f.dropoff_product_view_pct > a.avg_product_dropoff + (2 * a.std_product_dropoff) THEN 'High Product Drop-off'
        WHEN f.dropoff_add_to_cart_pct > a.avg_cart_dropoff + (2 * a.std_cart_dropoff) THEN 'High Cart Abandonment'
        ELSE 'Normal'
    END AS anomaly_type
FROM {{ ref('funnel_metrics') }} f
CROSS JOIN avg_metrics a
WHERE f.event_date >= DATEADD(day, -30, CURRENT_DATE())
    AND (
        f.dropoff_page_view_pct > a.avg_page_dropoff + (2 * a.std_page_dropoff)
        OR f.dropoff_product_view_pct > a.avg_product_dropoff + (2 * a.std_product_dropoff)
        OR f.dropoff_add_to_cart_pct > a.avg_cart_dropoff + (2 * a.std_cart_dropoff)
    )
ORDER BY f.event_date DESC;

-- ============================================================================
-- 5. MICRO-CONVERSIONS ANALYSIS
-- ============================================================================

-- Analyze engagement levels before purchase
SELECT 
    age_group,
    ROUND(AVG(total_page_views * 1.0 / NULLIF(users_page_view, 0)), 2) AS avg_pages_per_user,
    ROUND(AVG(total_product_views * 1.0 / NULLIF(users_product_view, 0)), 2) AS avg_products_per_user,
    ROUND(AVG(conversion_overall_pct), 2) AS avg_conversion_rate,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM {{ ref('funnel_metrics') }}
WHERE event_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY age_group
ORDER BY avg_conversion_rate DESC;

-- ============================================================================
-- 6. FUNNEL TRENDS OVER TIME
-- ============================================================================

-- Weekly funnel trends
SELECT 
    DATE_TRUNC('week', event_date) AS week,
    ROUND(AVG(conversion_overall_pct), 2) AS avg_conversion_rate,
    ROUND(AVG(conversion_cart_to_purchase_pct), 2) AS avg_cart_conversion,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    SUM(total_revenue) AS weekly_revenue,
    SUM(users_purchase) AS weekly_purchases
FROM {{ ref('funnel_metrics') }}
GROUP BY DATE_TRUNC('week', event_date)
ORDER BY week DESC
LIMIT 12;
