# Advanced SQL Analytics Techniques

This document explains the advanced SQL analytics patterns implemented in this project.

## Table of Contents
1. [Window Functions](#window-functions)
2. [Sessionization](#sessionization)
3. [Cohort Analysis](#cohort-analysis)
4. [Funnel Metrics](#funnel-metrics)

---

## Window Functions

Window functions perform calculations across a set of rows related to the current row, without collapsing the result set.

### Key Concepts

**Syntax:**
```sql
function_name() OVER (
    [PARTITION BY partition_expression]
    [ORDER BY sort_expression]
    [ROWS/RANGE frame_clause]
)
```

### Common Use Cases

#### 1. Running Totals

Calculate cumulative values over time:

```sql
SELECT 
    user_id,
    purchase_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales;
```

**Use Case:** Customer lifetime value tracking

#### 2. Moving Averages

Smooth out fluctuations in time series data:

```sql
SELECT 
    purchase_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY purchase_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day
FROM daily_sales;
```

**Use Case:** Trend analysis, seasonality detection

#### 3. Rankings

Assign ranks to rows within partitions:

```sql
SELECT 
    product_id,
    revenue,
    ROW_NUMBER() OVER (ORDER BY revenue DESC) AS row_num,
    RANK() OVER (ORDER BY revenue DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY revenue DESC) AS dense_rank,
    NTILE(4) OVER (ORDER BY revenue DESC) AS quartile
FROM product_sales;
```

**Differences:**
- `ROW_NUMBER()`: Unique sequential number (1, 2, 3, 4...)
- `RANK()`: Same rank for ties, gaps in sequence (1, 2, 2, 4...)
- `DENSE_RANK()`: Same rank for ties, no gaps (1, 2, 2, 3...)
- `NTILE(n)`: Divide into n equal buckets

#### 4. LAG and LEAD

Access previous or next row values:

```sql
SELECT 
    user_id,
    purchase_date,
    LAG(purchase_date) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
    ) AS previous_purchase_date,
    DATEDIFF(
        day,
        LAG(purchase_date) OVER (PARTITION BY user_id ORDER BY purchase_date),
        purchase_date
    ) AS days_since_last_purchase
FROM sales;
```

**Use Case:** Time between events, period-over-period comparisons

#### 5. FIRST_VALUE and LAST_VALUE

Get first or last value in a window:

```sql
SELECT 
    user_id,
    purchase_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_purchase_amount
FROM sales;
```

**Note:** Use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` for LAST_VALUE to avoid unexpected results.

### Window Frame Clauses

- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`: All rows from start to current
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`: Last 7 rows (including current)
- `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`: Current to end
- `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`: Previous, current, and next row

---

## Sessionization

Sessionization groups user events into sessions based on time gaps or business rules.

### Algorithm

1. **Order events** by user and timestamp
2. **Calculate time gap** from previous event
3. **Mark session boundaries** when gap exceeds threshold
4. **Assign session IDs** by cumulative sum of boundaries

### Implementation

```sql
WITH events_with_gaps AS (
    SELECT 
        user_id,
        event_timestamp,
        LAG(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) AS prev_event_time,
        DATEDIFF(
            minute,
            LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp),
            event_timestamp
        ) AS minutes_since_prev
    FROM events
),
session_starts AS (
    SELECT 
        *,
        CASE 
            WHEN prev_event_time IS NULL THEN 1
            WHEN minutes_since_prev > 30 THEN 1
            ELSE 0
        END AS is_session_start
    FROM events_with_gaps
),
sessions AS (
    SELECT 
        *,
        SUM(is_session_start) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM session_starts
)
SELECT 
    user_id,
    session_number,
    MIN(event_timestamp) AS session_start,
    MAX(event_timestamp) AS session_end,
    COUNT(*) AS events_in_session
FROM sessions
GROUP BY user_id, session_number;
```

### Key Metrics

- **Session Duration:** Time from first to last event
- **Events per Session:** Total events in session
- **Session Quality:** Based on actions taken (add to cart, purchase, etc.)
- **Bounce Rate:** Single-event sessions / total sessions

### Use Cases

- User engagement analysis
- A/B testing session-based metrics
- Conversion optimization
- UX improvement (identify friction points)

---

## Cohort Analysis

Cohort analysis groups users by shared characteristics (typically signup date) and tracks their behavior over time.

### Key Concepts

**Cohort:** Group of users who signed up in the same time period
**Cohort Age:** Time elapsed since cohort start
**Retention:** % of cohort still active at each cohort age

### Implementation

```sql
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', signup_date) AS cohort_month
    FROM users
),
cohort_activity AS (
    SELECT 
        c.cohort_month,
        c.user_id,
        DATE_TRUNC('month', s.purchase_date) AS activity_month,
        DATEDIFF(month, c.cohort_month, DATE_TRUNC('month', s.purchase_date)) AS cohort_age_months,
        s.net_amount
    FROM user_cohorts c
    JOIN sales s ON c.user_id = s.user_id
)
SELECT 
    cohort_month,
    cohort_age_months,
    COUNT(DISTINCT user_id) AS active_users,
    SUM(net_amount) AS revenue,
    SUM(net_amount) / COUNT(DISTINCT user_id) AS avg_revenue_per_user
FROM cohort_activity
GROUP BY cohort_month, cohort_age_months
ORDER BY cohort_month, cohort_age_months;
```

### Retention Calculation

```sql
WITH cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT user_id) AS size
    FROM user_cohorts
    GROUP BY cohort_month
)
SELECT 
    ca.cohort_month,
    ca.cohort_age_months,
    cs.size AS cohort_size,
    COUNT(DISTINCT ca.user_id) AS active_users,
    ROUND(100.0 * COUNT(DISTINCT ca.user_id) / cs.size, 2) AS retention_pct
FROM cohort_activity ca
JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
GROUP BY ca.cohort_month, ca.cohort_age_months, cs.size;
```

### Cohort Matrix (Pivot)

Create a retention matrix for visualization:

```sql
SELECT 
    cohort_month,
    MAX(CASE WHEN cohort_age_months = 0 THEN retention_pct END) AS month_0,
    MAX(CASE WHEN cohort_age_months = 1 THEN retention_pct END) AS month_1,
    MAX(CASE WHEN cohort_age_months = 2 THEN retention_pct END) AS month_2,
    MAX(CASE WHEN cohort_age_months = 3 THEN retention_pct END) AS month_3,
    MAX(CASE WHEN cohort_age_months = 6 THEN retention_pct END) AS month_6,
    MAX(CASE WHEN cohort_age_months = 12 THEN retention_pct END) AS month_12
FROM cohort_retention
GROUP BY cohort_month;
```

### Key Metrics

- **Retention Rate:** % of cohort active at each period
- **LTV (Lifetime Value):** Cumulative revenue per user
- **Cohort Revenue:** Total revenue from cohort
- **ARPU (Average Revenue Per User):** Revenue / cohort size

### Use Cases

- Product-market fit validation
- Feature impact analysis
- Marketing channel effectiveness
- Pricing strategy optimization
- Churn prediction

---

## Funnel Metrics

Funnel analysis tracks user progression through sequential steps toward a goal (e.g., purchase).

### Funnel Stages

Typical e-commerce funnel:
1. **Page View:** User visits site
2. **Product View:** User views product detail
3. **Add to Cart:** User adds item to cart
4. **Purchase:** User completes transaction

### Implementation

```sql
WITH user_funnel AS (
    SELECT 
        user_id,
        event_date,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS reached_page_view,
        MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS reached_product_view,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS reached_add_to_cart
    FROM events
    GROUP BY user_id, event_date
),
funnel_with_purchase AS (
    SELECT 
        f.*,
        CASE WHEN s.user_id IS NOT NULL THEN 1 ELSE 0 END AS reached_purchase
    FROM user_funnel f
    LEFT JOIN sales s ON f.user_id = s.user_id AND f.event_date = s.purchase_date
)
SELECT 
    event_date,
    SUM(reached_page_view) AS users_page_view,
    SUM(reached_product_view) AS users_product_view,
    SUM(reached_add_to_cart) AS users_add_to_cart,
    SUM(reached_purchase) AS users_purchase,
    ROUND(100.0 * SUM(reached_product_view) / NULLIF(SUM(reached_page_view), 0), 2) AS conversion_page_to_product,
    ROUND(100.0 * SUM(reached_purchase) / NULLIF(SUM(reached_page_view), 0), 2) AS conversion_overall
FROM funnel_with_purchase
GROUP BY event_date;
```

### Key Metrics

#### Conversion Rates
- **Step Conversion:** % moving from one step to next
- **Overall Conversion:** % completing entire funnel
- **Drop-off Rate:** % leaving at each step

#### Calculations

```sql
-- Step conversion
conversion_rate = (users_at_step_n / users_at_step_n-1) * 100

-- Drop-off rate
drop_off_rate = ((users_at_step_n-1 - users_at_step_n) / users_at_step_n-1) * 100

-- Overall conversion
overall_conversion = (users_at_final_step / users_at_first_step) * 100
```

### Segmented Funnel Analysis

Analyze funnels by segments to identify optimization opportunities:

```sql
SELECT 
    device_type,
    SUM(users_page_view) AS page_views,
    SUM(users_purchase) AS purchases,
    ROUND(100.0 * SUM(users_purchase) / NULLIF(SUM(users_page_view), 0), 2) AS conversion_rate
FROM funnel_metrics
GROUP BY device_type
ORDER BY conversion_rate DESC;
```

### Micro-Conversions

Track intermediate actions that predict final conversion:

- Email signups
- Account creation
- Wishlists/favorites
- Social shares
- Multiple product views

### Use Cases

- Identify optimization opportunities
- A/B test impact measurement
- UX improvements
- Marketing campaign effectiveness
- Feature prioritization

---

## Combining Techniques

### Example: Cohort Funnel Analysis

Analyze how cohort affects funnel conversion:

```sql
WITH user_cohorts AS (
    SELECT user_id, DATE_TRUNC('month', signup_date) AS cohort_month
    FROM users
),
funnel_by_cohort AS (
    SELECT 
        c.cohort_month,
        f.users_page_view,
        f.users_purchase,
        f.conversion_overall_pct
    FROM funnel_metrics f
    JOIN events e ON f.event_date = e.event_date
    JOIN user_cohorts c ON e.user_id = c.user_id
)
SELECT 
    cohort_month,
    AVG(conversion_overall_pct) AS avg_conversion_rate
FROM funnel_by_cohort
GROUP BY cohort_month
ORDER BY cohort_month;
```

### Example: Sessionized Cohort Analysis

Track session quality by cohort:

```sql
SELECT 
    c.cohort_month,
    AVG(s.engagement_score) AS avg_engagement,
    AVG(s.session_duration_minutes) AS avg_session_duration,
    COUNT(DISTINCT s.session_id) / COUNT(DISTINCT s.user_id) AS sessions_per_user
FROM sessionization s
JOIN users u ON s.user_id = u.user_id
JOIN (
    SELECT user_id, DATE_TRUNC('month', signup_date) AS cohort_month
    FROM users
) c ON s.user_id = c.user_id
GROUP BY c.cohort_month
ORDER BY c.cohort_month;
```

---

## Best Practices

### Performance
1. **Filter early:** Apply WHERE clauses before window functions
2. **Limit partitions:** Too many partitions can slow performance
3. **Use appropriate frame clauses:** Don't use unbounded frames unnecessarily
4. **Materialize results:** Cache expensive calculations

### Accuracy
1. **Handle NULL values:** Use NULLIF() or COALESCE()
2. **Avoid division by zero:** Use NULLIF(denominator, 0)
3. **Time zone awareness:** Standardize on UTC
4. **Date alignment:** Use DATE_TRUNC() for consistent periods

### Maintainability
1. **Use CTEs:** Break complex queries into logical steps
2. **Comment complex logic:** Explain business rules
3. **Parameterize:** Use variables for thresholds (session timeout, cohort periods)
4. **Test edge cases:** Empty cohorts, single-event sessions, etc.

---

## Resources

- **Window Functions:** [PostgreSQL Window Functions](https://www.postgresql.org/docs/current/tutorial-window.html)
- **Sessionization:** [Google Analytics Session Definition](https://support.google.com/analytics/answer/2731565)
- **Cohort Analysis:** [Amplitude Cohort Analysis Guide](https://amplitude.com/blog/cohort-analysis)
- **Funnel Analysis:** [Mixpanel Funnel Analysis](https://mixpanel.com/topics/what-is-funnel-analysis/)
