-- Cohort Analysis Query Examples
-- Analyzing user cohorts and retention patterns

-- ============================================================================
-- 1. BASIC COHORT RETENTION
-- ============================================================================

-- Monthly cohort retention rates
SELECT 
    cohort_month,
    cohort_age_months,
    cohort_size,
    active_users,
    retention_rate_pct,
    cumulative_revenue,
    ltv_to_date
FROM {{ ref('cohort_analysis') }}
WHERE region = 'North America'
    AND device_type = 'Mobile'
ORDER BY cohort_month, cohort_age_months;

-- ============================================================================
-- 2. COHORT RETENTION MATRIX (PIVOT VIEW)
-- ============================================================================

-- Create a retention matrix showing retention % by cohort and month
WITH retention_data AS (
    SELECT 
        cohort_month,
        cohort_age_months,
        retention_rate_pct
    FROM {{ ref('cohort_analysis') }}
    WHERE cohort_age_months <= 12
        AND region = 'North America'
)

SELECT 
    cohort_month,
    MAX(CASE WHEN cohort_age_months = 0 THEN retention_rate_pct END) AS month_0,
    MAX(CASE WHEN cohort_age_months = 1 THEN retention_rate_pct END) AS month_1,
    MAX(CASE WHEN cohort_age_months = 2 THEN retention_rate_pct END) AS month_2,
    MAX(CASE WHEN cohort_age_months = 3 THEN retention_rate_pct END) AS month_3,
    MAX(CASE WHEN cohort_age_months = 6 THEN retention_rate_pct END) AS month_6,
    MAX(CASE WHEN cohort_age_months = 12 THEN retention_rate_pct END) AS month_12
FROM retention_data
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================================
-- 3. COHORT LTV ANALYSIS
-- ============================================================================

-- Compare LTV across cohorts
SELECT 
    cohort_month,
    MAX(CASE WHEN cohort_age_months = 12 THEN ltv_to_date END) AS ltv_12_months,
    MAX(CASE WHEN cohort_age_months = 12 THEN cumulative_revenue END) AS revenue_12_months,
    MAX(CASE WHEN cohort_age_months = 12 THEN retention_rate_pct END) AS retention_12_months
FROM {{ ref('cohort_analysis') }}
GROUP BY cohort_month
HAVING MAX(CASE WHEN cohort_age_months = 12 THEN ltv_to_date END) IS NOT NULL
ORDER BY cohort_month;

-- ============================================================================
-- 4. COHORT COMPARISON BY SEGMENTS
-- ============================================================================

-- Compare cohort performance across regions
SELECT 
    region,
    cohort_month,
    cohort_age_months,
    cohort_size,
    retention_rate_pct,
    avg_revenue_per_user,
    ltv_to_date
FROM {{ ref('cohort_analysis') }}
WHERE cohort_age_months IN (0, 1, 3, 6, 12)
ORDER BY region, cohort_month, cohort_age_months;

-- ============================================================================
-- 5. EARLY INDICATOR ANALYSIS
-- ============================================================================

-- Analyze if first-month behavior predicts long-term retention
WITH first_month_metrics AS (
    SELECT 
        cohort_month,
        region,
        device_type,
        age_group,
        cohort_size,
        MAX(CASE WHEN cohort_age_months = 0 THEN avg_revenue_per_user END) AS month_0_arpu,
        MAX(CASE WHEN cohort_age_months = 0 THEN avg_transactions_per_user END) AS month_0_trans_per_user,
        MAX(CASE WHEN cohort_age_months = 6 THEN retention_rate_pct END) AS month_6_retention
    FROM {{ ref('cohort_analysis') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    CASE 
        WHEN month_0_arpu < 50 THEN 'Low First Purchase'
        WHEN month_0_arpu BETWEEN 50 AND 100 THEN 'Medium First Purchase'
        ELSE 'High First Purchase'
    END AS first_purchase_segment,
    COUNT(*) AS cohort_count,
    AVG(month_0_arpu) AS avg_first_month_arpu,
    AVG(month_6_retention) AS avg_6_month_retention
FROM first_month_metrics
WHERE month_6_retention IS NOT NULL
GROUP BY 
    CASE 
        WHEN month_0_arpu < 50 THEN 'Low First Purchase'
        WHEN month_0_arpu BETWEEN 50 AND 100 THEN 'Medium First Purchase'
        ELSE 'High First Purchase'
    END
ORDER BY avg_6_month_retention DESC;

-- ============================================================================
-- 6. COHORT REVENUE CURVES
-- ============================================================================

-- Show how revenue accumulates over cohort lifetime
SELECT 
    cohort_month,
    cohort_age_months,
    cumulative_revenue,
    cumulative_revenue / cohort_size AS revenue_per_user,
    (cumulative_revenue - LAG(cumulative_revenue) OVER (
        PARTITION BY cohort_month 
        ORDER BY cohort_age_months
    )) AS incremental_revenue,
    ROUND(
        100.0 * (cumulative_revenue - LAG(cumulative_revenue) OVER (
            PARTITION BY cohort_month 
            ORDER BY cohort_age_months
        )) / cumulative_revenue,
        2
    ) AS incremental_revenue_pct
FROM {{ ref('cohort_analysis') }}
WHERE region = 'North America'
ORDER BY cohort_month, cohort_age_months;
