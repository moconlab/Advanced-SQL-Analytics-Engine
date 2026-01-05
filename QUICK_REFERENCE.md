# Quick Reference Guide

## Common Commands

### Data Generation

**Snowflake:**
```bash
snowsql -f sql/data_generation/generate_synthetic_data_snowflake.sql
```

**BigQuery:**
```bash
bq query --use_legacy_sql=false < sql/data_generation/generate_synthetic_data_bigquery.sql
```

### dbt Commands

```bash
# Test connection
dbt debug

# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --select window_functions_analysis

# Run staging models only
dbt run --select staging.*

# Run tests
dbt test

# Full refresh (rebuild all models)
dbt run --full-refresh

# Generate documentation
dbt docs generate
dbt docs serve
```

### Example Queries

**Run all window function examples:**
```bash
# Snowflake
snowsql -f sql/queries/window_functions/window_functions_examples.sql

# BigQuery
bq query --use_legacy_sql=false < sql/queries/window_functions/window_functions_examples.sql
```

## Quick SQL Examples

### Customer Lifetime Value
```sql
SELECT 
    user_id,
    SUM(net_amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
    ) AS lifetime_value
FROM raw_sales;
```

### 7-Day Moving Average
```sql
SELECT 
    purchase_date,
    SUM(net_amount) AS daily_revenue,
    AVG(SUM(net_amount)) OVER (
        ORDER BY purchase_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day
FROM raw_sales
GROUP BY purchase_date;
```

### Session Count by User
```sql
SELECT 
    user_id,
    COUNT(*) AS session_count,
    AVG(session_duration_minutes) AS avg_duration
FROM sessionization
GROUP BY user_id
ORDER BY session_count DESC;
```

### Cohort Retention Rate
```sql
SELECT 
    cohort_month,
    cohort_age_months,
    ROUND(100.0 * active_users / cohort_size, 2) AS retention_pct
FROM cohort_analysis
WHERE cohort_age_months <= 12
ORDER BY cohort_month, cohort_age_months;
```

### Conversion Funnel
```sql
SELECT 
    device_type,
    SUM(users_page_view) AS page_views,
    SUM(users_purchase) AS purchases,
    ROUND(100.0 * SUM(users_purchase) / SUM(users_page_view), 2) AS conversion_rate
FROM funnel_metrics
GROUP BY device_type;
```

## File Locations

| What | Where |
|------|-------|
| Data generation | `sql/data_generation/` |
| dbt models | `dbt_project/models/` |
| Example queries | `sql/queries/` |
| Documentation | `docs/` |
| Configuration | `dbt_project/dbt_project.yml` |
| Credentials | `~/.dbt/profiles.yml` |

## Key Metrics

### Window Functions Model
- **user_lifetime_value**: Cumulative spend per user
- **purchase_number**: Nth purchase for user
- **category_7day_moving_avg**: 7-day revenue moving average
- **days_since_last_purchase**: Purchase frequency

### Sessionization Model
- **session_duration_minutes**: Length of session
- **engagement_score**: Quality indicator
- **events_in_session**: Activity level
- **session_quality**: High/Medium/Low Intent

### Cohort Analysis Model
- **retention_rate_pct**: % of cohort still active
- **ltv_to_date**: Lifetime value per user
- **avg_revenue_per_user**: ARPU by period
- **cumulative_revenue**: Total cohort revenue

### Funnel Metrics Model
- **conversion_overall_pct**: Page view → Purchase
- **conversion_cart_to_purchase_pct**: Cart → Purchase
- **dropoff_add_to_cart_pct**: Cart abandonment
- **avg_order_value**: AOV for converters

## Performance Tips

### Query Optimization
1. Always filter on partition columns (dates)
2. Select only needed columns (avoid SELECT *)
3. Filter before joins when possible
4. Use CTEs for readability

### Cost Optimization
**Snowflake:**
- Auto-suspend warehouses after 5 minutes
- Start with X-Small, scale up as needed
- Use result caching (automatic)
- Monitor with resource monitors

**BigQuery:**
- Partition large tables by date
- Cluster by filter columns
- Use APPROX_COUNT_DISTINCT for large datasets
- Set maximum bytes billed limits

## Troubleshooting

### dbt Connection Failed
1. Check `~/.dbt/profiles.yml` exists
2. Verify credentials are correct
3. Test database connection manually
4. Run `dbt debug` for details

### Query Too Slow
1. Check partitions are used
2. Verify clustering is applied
3. Review query execution plan
4. Consider materialized views

### Data Not Showing
1. Verify raw tables exist
2. Check dbt models ran successfully
3. Review dbt logs: `dbt_project/logs/`
4. Run with `--full-refresh` flag

## Data Volumes

| Table | Rows | Size (compressed) |
|-------|------|-------------------|
| raw_users | 50K | ~5 MB |
| raw_products | 1K | ~500 KB |
| raw_events | 5M | ~100 MB |
| raw_sales | 500K | ~10 MB |

## Environment Variables

### Snowflake
```bash
export SNOWFLAKE_ACCOUNT="<account>"
export SNOWFLAKE_USER="<username>"
export SNOWFLAKE_PASSWORD="<password>"
export SNOWFLAKE_ROLE="ANALYST"
export SNOWFLAKE_DATABASE="ANALYTICS"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

### BigQuery
```bash
export GCP_PROJECT="<project-id>"
export GCP_KEYFILE_PATH="$HOME/bigquery-key.json"
```

## Resources

- **Setup**: [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md)
- **Techniques**: [docs/ANALYTICS_TECHNIQUES.md](docs/ANALYTICS_TECHNIQUES.md)
- **Performance**: [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md)
- **Schema**: [docs/DATA_SCHEMA.md](docs/DATA_SCHEMA.md)

## Getting Help

1. Check documentation in `docs/` folder
2. Review example queries in `sql/queries/`
3. Check dbt logs for errors
4. Open GitHub issue with details

## Next Steps

1. ✅ Set up platform (Snowflake or BigQuery)
2. ✅ Generate synthetic data
3. ✅ Configure dbt
4. ✅ Run dbt models
5. ✅ Explore example queries
6. 🎯 Customize for your use case
7. 🎯 Connect to BI tool
8. 🎯 Add your own analytics
