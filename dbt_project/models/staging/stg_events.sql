-- Staging model for raw events data
-- This model cleans and standardizes event data from the raw layer

{{ config(
    materialized='view',
    tags=['staging', 'events']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_events') }}
),

cleaned AS (
    SELECT
        event_id,
        user_id,
        product_id,
        event_type,
        event_timestamp,
        event_date,
        session_duration_seconds,
        traffic_source,
        event_properties,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND event_timestamp IS NOT NULL
)

SELECT * FROM cleaned
