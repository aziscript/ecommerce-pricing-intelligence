WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'inventory_events') }}
)

SELECT
    event_id,
    product_id,
    warehouse_id,
    event_type,
    quantity_change,
    ABS(quantity_change)           AS absolute_quantity,
    timestamp::TIMESTAMPTZ         AS event_timestamp
FROM source
