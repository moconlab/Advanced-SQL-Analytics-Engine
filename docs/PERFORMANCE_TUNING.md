# Performance Tuning Guide

This guide covers performance optimization techniques for both Snowflake and BigQuery.

## Table of Contents
1. [Partitioning Strategies](#partitioning-strategies)
2. [Clustering and Indexing](#clustering-and-indexing)
3. [Query Optimization](#query-optimization)
4. [Materialized Views](#materialized-views)
5. [Monitoring and Profiling](#monitoring-and-profiling)

## Partitioning Strategies

### Snowflake
Snowflake uses micro-partitions automatically, but you can optimize with clustering:

```sql
-- Cluster by commonly filtered columns
ALTER TABLE raw_events CLUSTER BY (event_date, user_id);
```

**Benefits:**
- Reduces data scanning for filtered queries
- Improves join performance
- Automatic maintenance (Enterprise Edition)

**Best Practices:**
- Cluster by 3-4 columns max
- Put most selective columns first
- Monitor clustering depth (target < 4)

### BigQuery
BigQuery requires explicit partitioning:

```sql
-- Partition by date with clustering
CREATE TABLE events_partitioned
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_type
AS SELECT * FROM raw_events;
```

**Benefits:**
- Significant cost savings (partition pruning)
- Faster query performance
- Automatic partition management

**Best Practices:**
- Always partition large tables (>1GB)
- Use date/timestamp partitioning when possible
- Cluster by high-cardinality filter columns
- Maximum 4 clustering columns

## Clustering and Indexing

### Snowflake Clustering Keys

```sql
-- Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('raw_events', '(event_date, user_id)');

-- Enable automatic clustering
ALTER TABLE raw_events RESUME RECLUSTER;
```

**When to cluster:**
- Tables > 1TB
- Queries filter on specific columns
- Join performance is critical

### BigQuery Clustering

```sql
-- Cluster by up to 4 columns
CREATE TABLE sales_clustered
PARTITION BY DATE(purchase_timestamp)
CLUSTER BY user_id, product_id, region
AS SELECT * FROM raw_sales;
```

**When to cluster:**
- Tables > 1GB
- Queries filter/aggregate on specific columns
- High-cardinality columns

## Query Optimization

### 1. Avoid SELECT *

**Bad:**
```sql
SELECT * FROM events WHERE event_date = '2024-01-01';
```

**Good:**
```sql
SELECT event_id, user_id, event_type 
FROM events 
WHERE event_date = '2024-01-01';
```

**Savings:** Up to 90% in columnar databases

### 2. Filter Early

**Bad:**
```sql
SELECT * FROM sales s
JOIN users u ON s.user_id = u.user_id
WHERE s.purchase_date >= '2024-01-01';
```

**Good:**
```sql
WITH recent_sales AS (
    SELECT * FROM sales 
    WHERE purchase_date >= '2024-01-01'
)
SELECT * FROM recent_sales s
JOIN users u ON s.user_id = u.user_id;
```

### 3. Use Approximate Aggregations (BigQuery)

```sql
-- Exact (slower)
SELECT COUNT(DISTINCT user_id) FROM events;

-- Approximate (faster, 98%+ accurate)
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;
```

**Savings:** 50-90% faster for large datasets

### 4. Optimize Window Functions

**Bad:**
```sql
-- Unbounded window on large table
SELECT user_id, 
       AVG(amount) OVER (PARTITION BY user_id) 
FROM sales;
```

**Good:**
```sql
-- Pre-aggregate when possible
WITH user_avg AS (
    SELECT user_id, AVG(amount) AS avg_amount
    FROM sales
    GROUP BY user_id
)
SELECT s.*, u.avg_amount
FROM sales s
JOIN user_avg u ON s.user_id = u.user_id;
```

## Materialized Views

### Snowflake

```sql
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT 
    purchase_date,
    product_id,
    COUNT(*) AS sales_count,
    SUM(net_amount) AS revenue
FROM raw_sales
GROUP BY purchase_date, product_id;
```

**Cost:** Materialized views consume storage and compute for refreshes

### BigQuery

```sql
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT 
    DATE(purchase_timestamp) AS purchase_date,
    product_id,
    COUNT(*) AS sales_count,
    SUM(net_amount) AS revenue
FROM raw_sales
GROUP BY purchase_date, product_id;
```

**Cost:** Storage cost + automatic refresh cost

**When to use:**
- Repeated expensive aggregations
- Dashboard queries
- Complex joins used frequently

## Monitoring and Profiling

### Snowflake Query Profiling

```sql
-- Find slow queries
SELECT 
    query_id,
    query_text,
    execution_time,
    bytes_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE execution_time > 60000
ORDER BY execution_time DESC;

-- Check warehouse usage
SELECT 
    warehouse_name,
    SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;
```

### BigQuery Query Analysis

```sql
-- Find expensive queries
SELECT 
    creation_time,
    ROUND(total_bytes_processed / POW(1024, 3), 2) AS gb_processed,
    ROUND(total_slot_ms / 1000, 2) AS slot_seconds,
    query
FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY total_bytes_processed DESC
LIMIT 20;
```

## Performance Tuning Checklist

### Before Query Execution
- [ ] Are large tables partitioned?
- [ ] Are high-cardinality columns clustered?
- [ ] Am I selecting only needed columns?
- [ ] Are filters applied early?
- [ ] Can I use approximate aggregations?

### After Query Execution
- [ ] Check bytes scanned/processed
- [ ] Review query execution plan
- [ ] Identify full table scans
- [ ] Look for data spilling (Snowflake)
- [ ] Monitor slot usage (BigQuery)

### Regular Maintenance
- [ ] Review slow query log weekly
- [ ] Update clustering as query patterns change
- [ ] Archive old partitions
- [ ] Update materialized views as needed
- [ ] Monitor costs and set budgets

## Cost Optimization Tips

### Snowflake
1. **Right-size warehouses:** Start small, scale up as needed
2. **Auto-suspend:** Set to 1-5 minutes for dev, 5-10 for prod
3. **Use result cache:** Identical queries are free
4. **Monitor credit usage:** Set up resource monitors

### BigQuery
1. **Partition pruning:** Always filter on partition column
2. **Column selection:** Avoid SELECT * (columnar storage)
3. **Use slots efficiently:** Monitor slot usage
4. **Set query cost limits:** Use maximum bytes billed
5. **Approximate functions:** 10x cost savings for large aggregations

## Performance Benchmarks

### Query Performance Targets

| Query Type | Target Time | Notes |
|------------|-------------|-------|
| Simple filter | < 1 second | Single table, indexed columns |
| Aggregation | < 5 seconds | GROUP BY on clustered columns |
| Join (2 tables) | < 10 seconds | On clustered/partitioned columns |
| Window functions | < 30 seconds | With appropriate partitioning |
| Complex analytics | < 2 minutes | Multiple CTEs, window functions |

### Data Scanning Targets

| Table Size | Scan Target | Notes |
|------------|-------------|-------|
| < 1 GB | < 100 MB | Use partition/cluster filtering |
| 1-10 GB | < 500 MB | Critical to filter effectively |
| 10-100 GB | < 2 GB | Materialized views recommended |
| > 100 GB | < 5 GB | Incremental models required |

## Advanced Techniques

### 1. Incremental Models (dbt)

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail'
) }}

SELECT * FROM {{ ref('stg_sales') }}

{% if is_incremental() %}
WHERE purchase_date > (SELECT MAX(purchase_date) FROM {{ this }})
{% endif %}
```

### 2. Result Set Caching

Both platforms cache identical query results:
- **Snowflake:** 24 hours
- **BigQuery:** 24 hours

Use for dashboards and repeated analytics.

### 3. Denormalization

For heavily queried dimensions, denormalize to avoid joins:

```sql
CREATE TABLE sales_denormalized AS
SELECT 
    s.*,
    u.region,
    u.device_type,
    p.category,
    p.brand
FROM sales s
LEFT JOIN users u ON s.user_id = u.user_id
LEFT JOIN products p ON s.product_id = p.product_id;
```

**Trade-off:** Storage cost vs. query performance

## Resources

- [Snowflake Performance Tuning Guide](https://docs.snowflake.com/en/user-guide/performance-tuning)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
- [dbt Performance Tips](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-incremental-models)
