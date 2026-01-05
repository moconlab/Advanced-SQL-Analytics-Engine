-- Staging model for raw products data
-- This model cleans and standardizes product data from the raw layer

{{ config(
    materialized='view',
    tags=['staging', 'products']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_products') }}
),

cleaned AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        base_price,
        current_price,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned
