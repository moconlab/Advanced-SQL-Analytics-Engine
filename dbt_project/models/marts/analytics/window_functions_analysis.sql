-- Window Functions Analytics Model
-- Demonstrates advanced window functions for sales analytics
-- Including running totals, rankings, moving averages, and percentiles

{{ config(
    materialized='table',
    tags=['analytics', 'window_functions']
) }}

WITH daily_sales AS (
    SELECT
        s.purchase_date,
        s.user_id,
        s.product_id,
        p.category,
        s.net_amount,
        u.region
    FROM {{ ref('stg_sales') }} s
    LEFT JOIN {{ ref('stg_products') }} p ON s.product_id = p.product_id
    LEFT JOIN {{ ref('stg_users') }} u ON s.user_id = u.user_id
),

sales_with_windows AS (
    SELECT
        purchase_date,
        user_id,
        product_id,
        category,
        region,
        net_amount,
        
        -- Running totals
        SUM(net_amount) OVER (
            PARTITION BY user_id 
            ORDER BY purchase_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS user_lifetime_value,
        
        SUM(net_amount) OVER (
            PARTITION BY category 
            ORDER BY purchase_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS category_cumulative_revenue,
        
        -- Rankings
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY purchase_date
        ) AS purchase_number,
        
        DENSE_RANK() OVER (
            PARTITION BY category 
            ORDER BY net_amount DESC
        ) AS category_revenue_rank,
        
        -- Moving averages (7-day and 30-day)
        AVG(net_amount) OVER (
            PARTITION BY category
            ORDER BY purchase_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS category_7day_moving_avg,
        
        AVG(net_amount) OVER (
            PARTITION BY category
            ORDER BY purchase_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS category_30day_moving_avg,
        
        -- Lead and Lag for purchase patterns
        LAG(purchase_date, 1) OVER (
            PARTITION BY user_id 
            ORDER BY purchase_date
        ) AS previous_purchase_date,
        
        LEAD(purchase_date, 1) OVER (
            PARTITION BY user_id 
            ORDER BY purchase_date
        ) AS next_purchase_date,
        
        -- Percentiles
        PERCENT_RANK() OVER (
            PARTITION BY category 
            ORDER BY net_amount
        ) AS category_percentile,
        
        NTILE(4) OVER (
            PARTITION BY region 
            ORDER BY net_amount DESC
        ) AS region_quartile,
        
        -- First and Last values
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
        
    FROM daily_sales
),

final AS (
    SELECT
        *,
        -- Calculate days between purchases
        CASE 
            WHEN previous_purchase_date IS NOT NULL 
            THEN DATEDIFF(day, previous_purchase_date, purchase_date)
            ELSE NULL
        END AS days_since_last_purchase,
        
        -- Growth indicators
        CASE 
            WHEN first_purchase_amount > 0 
            THEN (last_purchase_amount - first_purchase_amount) / first_purchase_amount * 100
            ELSE 0
        END AS purchase_amount_growth_pct,
        
        -- Quartile labels
        CASE region_quartile
            WHEN 1 THEN 'Top 25%'
            WHEN 2 THEN 'Upper Middle 25%'
            WHEN 3 THEN 'Lower Middle 25%'
            WHEN 4 THEN 'Bottom 25%'
        END AS region_quartile_label
        
    FROM sales_with_windows
)

SELECT * FROM final
