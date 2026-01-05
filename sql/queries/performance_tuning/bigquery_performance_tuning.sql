-- Performance Tuning Examples for BigQuery
-- This file demonstrates various performance optimization techniques

-- ============================================================================
-- 1. PARTITIONING TABLES
-- ============================================================================

-- Create partitioned table by date
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.events_partitioned`
PARTITION BY DATE(event_timestamp)
AS SELECT * FROM `{{ project_id }}.analytics.raw_events`;

-- Create partitioned table with clustering
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.sales_partitioned`
PARTITION BY DATE(purchase_timestamp)
CLUSTER BY user_id, product_id
AS SELECT * FROM `{{ project_id }}.analytics.raw_sales`;

-- Check partition information
SELECT
    table_name,
    partition_id,
    total_rows,
    ROUND(total_logical_bytes / (1024*1024), 2) AS size_mb
FROM `{{ project_id }}.analytics.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'events_partitioned'
ORDER BY partition_id DESC
LIMIT 20;

-- ============================================================================
-- 2. CLUSTERING FOR HIGH-CARDINALITY COLUMNS
-- ============================================================================

-- Create table with clustering (up to 4 columns)
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.events_clustered`
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_type, traffic_source
AS SELECT * FROM `{{ project_id }}.analytics.raw_events`;

-- Check clustering info
SELECT
    table_name,
    clustering_ordinal_position,
    clustering_column_name
FROM `{{ project_id }}.analytics.INFORMATION_SCHEMA.CLUSTERING_COLUMNS`
WHERE table_name = 'events_clustered';

-- ============================================================================
-- 3. QUERY OPTIMIZATION TECHNIQUES
-- ============================================================================

-- Use partition filtering to reduce data scanned
-- Bad: Full table scan
SELECT COUNT(*) 
FROM `{{ project_id }}.analytics.raw_events`
WHERE user_id = 12345;

-- Good: Partition pruning
SELECT COUNT(*) 
FROM `{{ project_id }}.analytics.events_partitioned`
WHERE DATE(event_timestamp) >= '2024-01-01'
    AND user_id = 12345;

-- ============================================================================
-- 4. AVOID SELECT * WHEN POSSIBLE
-- ============================================================================

-- Bad: Scans all columns
SELECT * 
FROM `{{ project_id }}.analytics.raw_events`
WHERE event_date = '2024-01-01';

-- Good: Only needed columns
SELECT 
    event_id,
    user_id,
    event_type,
    event_timestamp
FROM `{{ project_id }}.analytics.raw_events`
WHERE event_date = '2024-01-01';

-- ============================================================================
-- 5. USE APPROXIMATE AGGREGATION FOR LARGE DATASETS
-- ============================================================================

-- Exact count (slower, more expensive)
SELECT COUNT(DISTINCT user_id) AS exact_users
FROM `{{ project_id }}.analytics.raw_events`;

-- Approximate count (faster, cheaper, 98%+ accuracy)
SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_users
FROM `{{ project_id }}.analytics.raw_events`;

-- Approximate quantiles
SELECT 
    APPROX_QUANTILES(net_amount, 100)[OFFSET(50)] AS median,
    APPROX_QUANTILES(net_amount, 100)[OFFSET(75)] AS p75,
    APPROX_QUANTILES(net_amount, 100)[OFFSET(95)] AS p95
FROM `{{ project_id }}.analytics.raw_sales`;

-- ============================================================================
-- 6. MATERIALIZED VIEWS FOR FREQUENT QUERIES
-- ============================================================================

-- Create materialized view for expensive aggregations
CREATE MATERIALIZED VIEW `{{ project_id }}.analytics.mv_daily_sales_summary`
AS
SELECT 
    DATE(purchase_timestamp) AS purchase_date,
    product_id,
    COUNT(*) AS transaction_count,
    SUM(quantity) AS total_quantity,
    SUM(net_amount) AS total_revenue,
    AVG(net_amount) AS avg_order_value
FROM `{{ project_id }}.analytics.raw_sales`
GROUP BY purchase_date, product_id;

-- Query the materialized view (automatic refresh)
SELECT * 
FROM `{{ project_id }}.analytics.mv_daily_sales_summary`
WHERE purchase_date >= '2024-01-01'
ORDER BY total_revenue DESC;

-- ============================================================================
-- 7. OPTIMIZE JOINS
-- ============================================================================

-- Use INNER JOIN instead of WHERE for joins
-- Bad: Old-style join
SELECT s.*, u.region
FROM `{{ project_id }}.analytics.raw_sales` s,
     `{{ project_id }}.analytics.raw_users` u
WHERE s.user_id = u.user_id
    AND s.purchase_date >= '2024-01-01';

-- Good: Explicit INNER JOIN with early filtering
SELECT s.*, u.region
FROM (
    SELECT * FROM `{{ project_id }}.analytics.raw_sales`
    WHERE purchase_date >= '2024-01-01'
) s
INNER JOIN `{{ project_id }}.analytics.raw_users` u 
    ON s.user_id = u.user_id;

-- ============================================================================
-- 8. USE ARRAYS AND STRUCTS EFFICIENTLY
-- ============================================================================

-- Denormalize with STRUCT for better performance
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.sales_denormalized` AS
SELECT 
    s.*,
    STRUCT(
        u.region,
        u.age_group,
        u.device_type
    ) AS user_info,
    STRUCT(
        p.category,
        p.brand,
        p.current_price
    ) AS product_info
FROM `{{ project_id }}.analytics.raw_sales` s
LEFT JOIN `{{ project_id }}.analytics.raw_users` u ON s.user_id = u.user_id
LEFT JOIN `{{ project_id }}.analytics.raw_products` p ON s.product_id = p.product_id;

-- Query denormalized table (faster than joins)
SELECT 
    purchase_date,
    user_info.region,
    product_info.category,
    SUM(net_amount) AS revenue
FROM `{{ project_id }}.analytics.sales_denormalized`
GROUP BY 1, 2, 3;

-- ============================================================================
-- 9. QUERY RESULTS CACHING
-- ============================================================================

-- BigQuery automatically caches query results for 24 hours
-- Identical queries will use cached results (free)

-- Force cache bypass for testing
SELECT * 
FROM `{{ project_id }}.analytics.raw_sales`
WHERE RAND() > 0  -- This prevents cache usage
    AND purchase_date >= '2024-01-01';

-- ============================================================================
-- 10. BI ENGINE OPTIMIZATION
-- ============================================================================

-- Reserve BI Engine capacity for faster queries
-- Note: This requires BI Engine to be enabled in GCP Console

-- Check BI Engine usage
SELECT 
    creation_time,
    project_id,
    bi_engine_statistics.bi_engine_mode,
    bi_engine_statistics.bi_engine_reasons,
    total_bytes_processed,
    total_slot_ms
FROM `{{ project_id }}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND bi_engine_statistics.bi_engine_mode IS NOT NULL
ORDER BY creation_time DESC
LIMIT 100;

-- ============================================================================
-- 11. QUERY PROFILING AND MONITORING
-- ============================================================================

-- Analyze query execution details
SELECT 
    creation_time,
    job_id,
    user_email,
    ROUND(total_bytes_processed / (1024*1024*1024), 2) AS gb_processed,
    ROUND(total_slot_ms / 1000, 2) AS slot_seconds,
    ROUND(total_bytes_billed / (1024*1024*1024), 2) AS gb_billed,
    statement_type,
    query
FROM `{{ project_id }}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND statement_type = 'SELECT'
ORDER BY total_bytes_processed DESC
LIMIT 20;

-- Find expensive queries
SELECT 
    DATE(creation_time) AS query_date,
    COUNT(*) AS query_count,
    ROUND(SUM(total_bytes_processed) / (1024*1024*1024*1024), 2) AS total_tb_processed,
    ROUND(SUM(total_bytes_billed) / (1024*1024*1024*1024), 2) AS total_tb_billed,
    ROUND(AVG(total_slot_ms) / 1000, 2) AS avg_slot_seconds
FROM `{{ project_id }}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND statement_type = 'SELECT'
    AND state = 'DONE'
GROUP BY query_date
ORDER BY query_date DESC;

-- ============================================================================
-- 12. TABLE OPTIMIZATION
-- ============================================================================

-- Check table size and details
SELECT 
    table_schema,
    table_name,
    ROUND(size_bytes / (1024*1024*1024), 2) AS size_gb,
    row_count,
    creation_time,
    type
FROM `{{ project_id }}.analytics.__TABLES__`
ORDER BY size_bytes DESC;

-- Check partition expiration and retention
SELECT 
    table_name,
    partition_id,
    total_rows,
    ROUND(total_logical_bytes / (1024*1024), 2) AS size_mb,
    last_modified_time
FROM `{{ project_id }}.analytics.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ('events_partitioned', 'sales_partitioned')
ORDER BY table_name, partition_id DESC;

-- ============================================================================
-- 13. BEST PRACTICES SUMMARY
-- ============================================================================

-- 1. Always partition large tables by date
-- 2. Use clustering for high-cardinality filter columns (up to 4)
-- 3. Avoid SELECT * - only select needed columns
-- 4. Use approximate aggregations for large datasets
-- 5. Create materialized views for expensive repeated queries
-- 6. Use STRUCT to denormalize related data
-- 7. Filter on partition columns first
-- 8. Monitor query costs and optimize expensive queries
-- 9. Leverage query result caching (automatic)
-- 10. Consider BI Engine for interactive dashboards
