-- Sessionization Query Examples
-- Demonstrating how to group events into sessions

-- ============================================================================
-- 1. BASIC SESSIONIZATION WITH 30-MINUTE TIMEOUT
-- ============================================================================

WITH events_with_gaps AS (
    SELECT 
        event_id,
        user_id,
        event_timestamp,
        event_type,
        LAG(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) AS prev_event_time,
        DATEDIFF(
            minute,
            LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp),
            event_timestamp
        ) AS minutes_since_prev_event
    FROM raw_events
),

session_starts AS (
    SELECT 
        *,
        CASE 
            WHEN prev_event_time IS NULL THEN 1
            WHEN minutes_since_prev_event > 30 THEN 1
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
    COUNT(*) AS events_in_session,
    COUNT(DISTINCT product_id) AS products_viewed,
    DATEDIFF(minute, MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_minutes
FROM sessions
GROUP BY user_id, session_number
ORDER BY user_id, session_number;

-- ============================================================================
-- 2. SESSION METRICS BY DEVICE AND TRAFFIC SOURCE
-- ============================================================================

WITH sessionized_events AS (
    -- Use the sessionization logic from above
    SELECT 
        e.*,
        u.device_type,
        e.traffic_source,
        SUM(
            CASE 
                WHEN LAG(e.event_timestamp) OVER (PARTITION BY e.user_id ORDER BY e.event_timestamp) IS NULL THEN 1
                WHEN DATEDIFF(minute, LAG(e.event_timestamp) OVER (PARTITION BY e.user_id ORDER BY e.event_timestamp), e.event_timestamp) > 30 THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY e.user_id 
            ORDER BY e.event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM raw_events e
    JOIN raw_users u ON e.user_id = u.user_id
)

SELECT 
    device_type,
    traffic_source,
    COUNT(DISTINCT CONCAT(user_id, '-', session_number)) AS total_sessions,
    AVG(session_events) AS avg_events_per_session,
    AVG(session_duration_minutes) AS avg_session_duration,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS sessions_with_purchase,
    ROUND(
        100.0 * SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) / 
        COUNT(DISTINCT CONCAT(user_id, '-', session_number)),
        2
    ) AS conversion_rate_pct
FROM (
    SELECT 
        user_id,
        session_number,
        device_type,
        traffic_source,
        COUNT(*) AS session_events,
        DATEDIFF(minute, MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_minutes,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_purchase
    FROM sessionized_events
    GROUP BY user_id, session_number, device_type, traffic_source
) session_stats
GROUP BY device_type, traffic_source
ORDER BY total_sessions DESC;

-- ============================================================================
-- 3. USER SESSION PATTERNS ANALYSIS
-- ============================================================================

WITH user_sessions AS (
    SELECT 
        user_id,
        COUNT(DISTINCT session_id) AS total_sessions,
        AVG(events_in_session) AS avg_events_per_session,
        AVG(session_duration_minutes) AS avg_session_duration,
        SUM(events_in_session) AS total_events
    FROM (
        -- Simplified: use the sessionization model if available
        SELECT 
            session_id,
            user_id,
            events_in_session,
            session_duration_minutes
        FROM {{ ref('sessionization') }}
    ) sessions
    GROUP BY user_id
)

SELECT 
    CASE 
        WHEN total_sessions = 1 THEN 'Single Session'
        WHEN total_sessions BETWEEN 2 AND 5 THEN '2-5 Sessions'
        WHEN total_sessions BETWEEN 6 AND 10 THEN '6-10 Sessions'
        ELSE '10+ Sessions'
    END AS session_frequency_bucket,
    COUNT(*) AS user_count,
    AVG(total_sessions) AS avg_sessions,
    AVG(avg_events_per_session) AS avg_events,
    AVG(avg_session_duration) AS avg_duration
FROM user_sessions
GROUP BY 
    CASE 
        WHEN total_sessions = 1 THEN 'Single Session'
        WHEN total_sessions BETWEEN 2 AND 5 THEN '2-5 Sessions'
        WHEN total_sessions BETWEEN 6 AND 10 THEN '6-10 Sessions'
        ELSE '10+ Sessions'
    END
ORDER BY 
    MIN(total_sessions);

-- ============================================================================
-- 4. SESSION ABANDONMENT ANALYSIS
-- ============================================================================

SELECT 
    session_quality,
    COUNT(*) AS session_count,
    AVG(session_duration_minutes) AS avg_duration,
    AVG(events_in_session) AS avg_events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total_sessions
FROM {{ ref('sessionization') }}
GROUP BY session_quality
ORDER BY session_count DESC;
