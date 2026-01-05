-- Cohort Analysis Model
-- Analyzes user retention and behavior by signup cohort
-- Tracks cohort performance over time

{{ config(
    materialized='table',
    tags=['analytics', 'cohort_analysis']
) }}

WITH user_cohorts AS (
    SELECT
        user_id,
        cohort_month,
        region,
        device_type,
        age_group
    FROM {{ ref('stg_users') }}
),

user_purchases AS (
    SELECT
        s.user_id,
        s.purchase_date,
        DATE_TRUNC('month', s.purchase_date) AS purchase_month,
        s.net_amount
    FROM {{ ref('stg_sales') }} s
),

cohort_purchases AS (
    SELECT
        uc.cohort_month,
        uc.user_id,
        uc.region,
        uc.device_type,
        uc.age_group,
        up.purchase_month,
        up.net_amount,
        
        -- Calculate period number (months since cohort start)
        DATEDIFF(month, uc.cohort_month, up.purchase_month) AS cohort_age_months
        
    FROM user_cohorts uc
    INNER JOIN user_purchases up ON uc.user_id = up.user_id
),

cohort_metrics AS (
    SELECT
        cohort_month,
        cohort_age_months,
        region,
        device_type,
        age_group,
        
        -- User counts
        COUNT(DISTINCT user_id) AS active_users,
        
        -- Revenue metrics
        SUM(net_amount) AS total_revenue,
        AVG(net_amount) AS avg_revenue_per_transaction,
        SUM(net_amount) / COUNT(DISTINCT user_id) AS avg_revenue_per_user,
        
        -- Transaction metrics
        COUNT(*) AS total_transactions,
        COUNT(*) / COUNT(DISTINCT user_id) AS avg_transactions_per_user
        
    FROM cohort_purchases
    GROUP BY 1, 2, 3, 4, 5
),

cohort_size AS (
    SELECT
        cohort_month,
        region,
        device_type,
        age_group,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM user_cohorts
    GROUP BY 1, 2, 3, 4
),

final AS (
    SELECT
        cm.cohort_month,
        cm.cohort_age_months,
        cm.region,
        cm.device_type,
        cm.age_group,
        cs.cohort_size,
        cm.active_users,
        cm.total_revenue,
        cm.avg_revenue_per_transaction,
        cm.avg_revenue_per_user,
        cm.total_transactions,
        cm.avg_transactions_per_user,
        
        -- Retention rate
        ROUND(100.0 * cm.active_users / cs.cohort_size, 2) AS retention_rate_pct,
        
        -- Cumulative metrics
        SUM(cm.total_revenue) OVER (
            PARTITION BY cm.cohort_month, cm.region, cm.device_type, cm.age_group
            ORDER BY cm.cohort_age_months
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_revenue,
        
        SUM(cm.active_users) OVER (
            PARTITION BY cm.cohort_month, cm.region, cm.device_type, cm.age_group
            ORDER BY cm.cohort_age_months
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_active_users,
        
        -- LTV estimate (total revenue up to this point / cohort size)
        SUM(cm.total_revenue) OVER (
            PARTITION BY cm.cohort_month, cm.region, cm.device_type, cm.age_group
            ORDER BY cm.cohort_age_months
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / cs.cohort_size AS ltv_to_date,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM cohort_metrics cm
    LEFT JOIN cohort_size cs 
        ON cm.cohort_month = cs.cohort_month
        AND cm.region = cs.region
        AND cm.device_type = cs.device_type
        AND cm.age_group = cs.age_group
)

SELECT * FROM final
WHERE cohort_age_months >= 0
ORDER BY cohort_month, cohort_age_months
