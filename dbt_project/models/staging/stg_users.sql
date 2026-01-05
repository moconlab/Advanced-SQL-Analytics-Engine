-- Staging model for raw users data
-- This model cleans and standardizes user data from the raw layer

{{ config(
    materialized='view',
    tags=['staging', 'users']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_users') }}
),

cleaned AS (
    SELECT
        user_id,
        user_email,
        age,
        age_group,
        region,
        device_type,
        signup_date,
        cohort_month,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE user_id IS NOT NULL
)

SELECT * FROM cleaned
