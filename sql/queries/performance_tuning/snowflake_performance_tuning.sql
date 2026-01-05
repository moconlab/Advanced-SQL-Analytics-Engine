-- Performance Tuning Examples for Snowflake
-- This file demonstrates various performance optimization techniques

-- ============================================================================
-- 1. CLUSTERING KEYS FOR IMPROVED QUERY PERFORMANCE
-- ============================================================================

-- Cluster tables by commonly filtered columns
ALTER TABLE raw_events CLUSTER BY (event_date, user_id);
ALTER TABLE raw_sales CLUSTER BY (purchase_date, user_id);

-- Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('raw_events', '(event_date, user_id)');
SELECT SYSTEM$CLUSTERING_INFORMATION('raw_sales', '(purchase_date, user_id)');

-- Verify clustering depth (lower is better, ideally < 4)
SELECT 
    table_name,
    clustering_key,
    average_depth,
    average_overlaps
FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    TABLE_NAME => 'raw_events'
));

-- ============================================================================
-- 2. PARTITIONING STRATEGIES (via CLUSTERING)
-- ============================================================================

-- For time-series data, cluster by date
ALTER TABLE raw_events CLUSTER BY (DATE_TRUNC('month', event_date));

-- For high-cardinality joins, use multi-column clustering
ALTER TABLE raw_sales CLUSTER BY (user_id, product_id, purchase_date);

-- Automatic clustering (Enterprise Edition)
ALTER TABLE raw_events RESUME RECLUSTER;
ALTER TABLE raw_sales RESUME RECLUSTER;

-- ============================================================================
-- 3. RESULT CACHING
-- ============================================================================

-- Enable result caching for repeated queries
-- Results are cached for 24 hours by default
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Example: This query will use cached results if run within 24 hours
SELECT 
    purchase_date,
    COUNT(*) AS total_sales,
    SUM(net_amount) AS total_revenue
FROM raw_sales
WHERE purchase_date >= '2024-01-01'
GROUP BY purchase_date;

-- ============================================================================
-- 4. MATERIALIZED VIEWS FOR EXPENSIVE AGGREGATIONS
-- ============================================================================

-- Create materialized view for frequently accessed metrics
CREATE OR REPLACE MATERIALIZED VIEW mv_daily_sales_summary AS
SELECT 
    purchase_date,
    product_id,
    COUNT(*) AS transaction_count,
    SUM(quantity) AS total_quantity,
    SUM(net_amount) AS total_revenue,
    AVG(net_amount) AS avg_order_value
FROM raw_sales
GROUP BY purchase_date, product_id;

-- Refresh materialized view (automatic in Enterprise Edition)
ALTER MATERIALIZED VIEW mv_daily_sales_summary SUSPEND;
ALTER MATERIALIZED VIEW mv_daily_sales_summary RESUME;

-- ============================================================================
-- 5. QUERY OPTIMIZATION TECHNIQUES
-- ============================================================================

-- Use CTEs for better readability and optimization
-- Bad: Nested subqueries
SELECT * FROM (
    SELECT * FROM (
        SELECT user_id, SUM(net_amount) AS total 
        FROM raw_sales 
        GROUP BY user_id
    ) WHERE total > 1000
) ORDER BY total DESC;

-- Good: CTEs
WITH user_totals AS (
    SELECT 
        user_id, 
        SUM(net_amount) AS total 
    FROM raw_sales 
    GROUP BY user_id
),
high_value_users AS (
    SELECT * FROM user_totals WHERE total > 1000
)
SELECT * FROM high_value_users ORDER BY total DESC;

-- ============================================================================
-- 6. PREDICATE PUSHDOWN
-- ============================================================================

-- Filter early to reduce data scanned
-- Bad: Filter after join
SELECT s.*, u.region
FROM raw_sales s
JOIN raw_users u ON s.user_id = u.user_id
WHERE s.purchase_date >= '2024-01-01';

-- Good: Filter before join
WITH recent_sales AS (
    SELECT * FROM raw_sales
    WHERE purchase_date >= '2024-01-01'
)
SELECT s.*, u.region
FROM recent_sales s
JOIN raw_users u ON s.user_id = u.user_id;

-- ============================================================================
-- 7. AVOID SELECT * WHEN POSSIBLE
-- ============================================================================

-- Bad: Retrieves all columns
SELECT * FROM raw_events WHERE event_date = '2024-01-01';

-- Good: Only select needed columns
SELECT 
    event_id,
    user_id,
    event_type,
    event_timestamp
FROM raw_events 
WHERE event_date = '2024-01-01';

-- ============================================================================
-- 8. USE APPROPRIATE DATA TYPES
-- ============================================================================

-- Use smaller data types when possible to reduce storage and improve performance
-- Example: Store dates as DATE not VARCHAR
-- Example: Use INTEGER not VARCHAR for numeric IDs

-- Check column statistics
SELECT 
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'PUBLIC'
    AND table_name IN ('RAW_SALES', 'RAW_EVENTS');

-- ============================================================================
-- 9. SEMI-STRUCTURED DATA OPTIMIZATION
-- ============================================================================

-- Extract commonly used fields from VARIANT columns
-- Bad: Query VARIANT directly
SELECT 
    event_properties:page_url::STRING AS page_url
FROM raw_events;

-- Good: Create computed columns
ALTER TABLE raw_events 
ADD COLUMN page_url VARCHAR AS (event_properties:page_url::STRING);

-- ============================================================================
-- 10. QUERY PROFILING AND ANALYSIS
-- ============================================================================

-- Get query profile for last query
SELECT 
    query_id,
    query_text,
    execution_status,
    total_elapsed_time,
    bytes_scanned,
    rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%raw_sales%'
ORDER BY start_time DESC
LIMIT 10;

-- Analyze partition pruning effectiveness
SELECT 
    query_id,
    query_text,
    partitions_scanned,
    partitions_total,
    ROUND(100.0 * partitions_scanned / NULLIF(partitions_total, 0), 2) AS pruning_effectiveness
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE partitions_total > 0
ORDER BY start_time DESC
LIMIT 10;

-- ============================================================================
-- 11. WAREHOUSE SIZING
-- ============================================================================

-- Monitor warehouse usage
SELECT 
    warehouse_name,
    AVG(avg_running) AS avg_queries_running,
    AVG(avg_queued_load) AS avg_queued,
    SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;

-- Identify slow queries
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    execution_time,
    queued_overload_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE execution_time > 60000  -- Queries taking more than 60 seconds
ORDER BY execution_time DESC
LIMIT 20;

-- ============================================================================
-- 12. TABLE OPTIMIZATION COMMANDS
-- ============================================================================

-- Analyze table for optimization opportunities
-- Check table size and row count
SELECT 
    table_name,
    row_count,
    bytes,
    ROUND(bytes / (1024*1024*1024), 2) AS size_gb
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'PUBLIC'
    AND table_type = 'BASE TABLE'
ORDER BY bytes DESC;

-- Monitor micro-partitions
SELECT 
    table_name,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    retained_for_clone_bytes
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog = CURRENT_DATABASE()
    AND table_schema = 'PUBLIC'
ORDER BY active_bytes DESC;
