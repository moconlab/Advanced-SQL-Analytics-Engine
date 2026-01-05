-- Staging model for raw sales data
-- This model cleans and standardizes sales data from the raw layer

{{ config(
    materialized='view',
    tags=['staging', 'sales']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_sales') }}
),

cleaned AS (
    SELECT
        sale_id,
        user_id,
        product_id,
        purchase_timestamp,
        purchase_date,
        quantity,
        current_price,
        total_amount,
        discount_amount,
        net_amount,
        payment_method,
        order_status,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE sale_id IS NOT NULL
        AND user_id IS NOT NULL
        AND order_status = 'completed'  -- Filter out refunded orders
)

SELECT * FROM cleaned
