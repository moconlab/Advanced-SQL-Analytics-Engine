-- Sessionization Model
-- Groups user events into sessions based on time gaps
-- A session ends when there's a gap of more than 30 minutes between events

{{ config(
    materialized='table',
    tags=['analytics', 'sessionization']
) }}

WITH events_ordered AS (
    SELECT
        e.event_id,
        e.user_id,
        e.event_type,
        e.event_timestamp,
        e.product_id,
        e.traffic_source,
        u.region,
        u.device_type,
        
        -- Get previous event timestamp for the same user
        LAG(e.event_timestamp) OVER (
            PARTITION BY e.user_id 
            ORDER BY e.event_timestamp
        ) AS prev_event_timestamp
        
    FROM {{ ref('stg_events') }} e
    LEFT JOIN {{ ref('stg_users') }} u ON e.user_id = u.user_id
),

session_boundaries AS (
    SELECT
        *,
        -- Calculate time difference from previous event in minutes
        CASE 
            WHEN prev_event_timestamp IS NULL THEN 0
            ELSE DATEDIFF(minute, prev_event_timestamp, event_timestamp)
        END AS minutes_since_prev_event,
        
        -- Mark session start (new session if gap > 30 minutes or first event)
        CASE 
            WHEN prev_event_timestamp IS NULL THEN 1
            WHEN DATEDIFF(minute, prev_event_timestamp, event_timestamp) > {{ var('session_timeout_minutes', 30) }} THEN 1
            ELSE 0
        END AS is_session_start
        
    FROM events_ordered
),

sessions_numbered AS (
    SELECT
        *,
        -- Create session ID by summing session starts up to current row
        SUM(is_session_start) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS user_session_number
        
    FROM session_boundaries
),

session_aggregates AS (
    SELECT
        CONCAT(user_id, '-', user_session_number) AS session_id,
        user_id,
        user_session_number,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        COUNT(*) AS events_in_session,
        COUNT(DISTINCT product_id) AS unique_products_viewed,
        MAX(region) AS region,
        MAX(device_type) AS device_type,
        MAX(traffic_source) AS traffic_source,
        
        -- Event type breakdown
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
        SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_events,
        
        -- Session quality indicators
        CASE 
            WHEN SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) > 0 THEN 1 
            ELSE 0 
        END AS has_cart_activity
        
    FROM sessions_numbered
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        *,
        DATEDIFF(second, session_start, session_end) AS session_duration_seconds,
        DATEDIFF(minute, session_start, session_end) AS session_duration_minutes,
        
        -- Engagement score (weighted by different event types)
        (page_views * 1) + 
        (product_views * 2) + 
        (add_to_cart_events * 5) - 
        (remove_from_cart_events * 3) AS engagement_score,
        
        -- Session quality classification
        CASE 
            WHEN add_to_cart_events > 0 THEN 'High Intent'
            WHEN product_views > 3 THEN 'Medium Intent'
            WHEN page_views > 5 THEN 'Browsing'
            ELSE 'Low Engagement'
        END AS session_quality,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM session_aggregates
)

SELECT * FROM final
