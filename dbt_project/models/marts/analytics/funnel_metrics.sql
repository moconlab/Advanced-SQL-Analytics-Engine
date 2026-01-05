-- Funnel Metrics Model
-- Analyzes conversion funnels from page view to purchase
-- Calculates conversion rates at each stage

{{ config(
    materialized='table',
    tags=['analytics', 'funnel_metrics']
) }}

WITH user_events AS (
    SELECT
        e.user_id,
        e.event_date,
        e.event_type,
        e.product_id,
        e.event_timestamp,
        u.region,
        u.device_type,
        u.age_group
    FROM {{ ref('stg_events') }} e
    LEFT JOIN {{ ref('stg_users') }} u ON e.user_id = u.user_id
),

-- Define funnel stages for each user and date
daily_user_funnel AS (
    SELECT
        user_id,
        event_date,
        region,
        device_type,
        age_group,
        
        -- Funnel Stage 1: Page View
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS reached_page_view,
        
        -- Funnel Stage 2: Product View
        MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS reached_product_view,
        
        -- Funnel Stage 3: Add to Cart
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS reached_add_to_cart,
        
        -- Count events by type
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_view_count,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count
        
    FROM user_events
    GROUP BY 1, 2, 3, 4, 5
),

-- Add purchase data (Funnel Stage 4)
user_funnel_with_purchases AS (
    SELECT
        f.*,
        CASE WHEN s.user_id IS NOT NULL THEN 1 ELSE 0 END AS reached_purchase,
        COALESCE(COUNT(s.sale_id), 0) AS purchase_count,
        COALESCE(SUM(s.net_amount), 0) AS purchase_amount
    FROM daily_user_funnel f
    LEFT JOIN {{ ref('stg_sales') }} s 
        ON f.user_id = s.user_id 
        AND f.event_date = s.purchase_date
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

-- Aggregate funnel metrics by date and dimensions
funnel_summary AS (
    SELECT
        event_date,
        region,
        device_type,
        age_group,
        
        -- Stage counts
        COUNT(DISTINCT user_id) AS total_users,
        SUM(reached_page_view) AS users_page_view,
        SUM(reached_product_view) AS users_product_view,
        SUM(reached_add_to_cart) AS users_add_to_cart,
        SUM(reached_purchase) AS users_purchase,
        
        -- Event counts
        SUM(page_view_count) AS total_page_views,
        SUM(product_view_count) AS total_product_views,
        SUM(add_to_cart_count) AS total_add_to_cart,
        SUM(purchase_count) AS total_purchases,
        
        -- Revenue
        SUM(purchase_amount) AS total_revenue
        
    FROM user_funnel_with_purchases
    GROUP BY 1, 2, 3, 4
),

final AS (
    SELECT
        event_date,
        region,
        device_type,
        age_group,
        total_users,
        users_page_view,
        users_product_view,
        users_add_to_cart,
        users_purchase,
        total_page_views,
        total_product_views,
        total_add_to_cart,
        total_purchases,
        total_revenue,
        
        -- Conversion rates (stage to stage)
        ROUND(100.0 * users_product_view / NULLIF(users_page_view, 0), 2) AS conversion_page_to_product_pct,
        ROUND(100.0 * users_add_to_cart / NULLIF(users_product_view, 0), 2) AS conversion_product_to_cart_pct,
        ROUND(100.0 * users_purchase / NULLIF(users_add_to_cart, 0), 2) AS conversion_cart_to_purchase_pct,
        
        -- Overall conversion rate (top to bottom)
        ROUND(100.0 * users_purchase / NULLIF(users_page_view, 0), 2) AS conversion_overall_pct,
        
        -- Drop-off rates
        ROUND(100.0 * (users_page_view - users_product_view) / NULLIF(users_page_view, 0), 2) AS dropoff_page_view_pct,
        ROUND(100.0 * (users_product_view - users_add_to_cart) / NULLIF(users_product_view, 0), 2) AS dropoff_product_view_pct,
        ROUND(100.0 * (users_add_to_cart - users_purchase) / NULLIF(users_add_to_cart, 0), 2) AS dropoff_add_to_cart_pct,
        
        -- Average metrics
        ROUND(total_revenue / NULLIF(users_purchase, 0), 2) AS avg_order_value,
        ROUND(total_revenue / NULLIF(total_users, 0), 2) AS revenue_per_user,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM funnel_summary
)

SELECT * FROM final
ORDER BY event_date DESC
