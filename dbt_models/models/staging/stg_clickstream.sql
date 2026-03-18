WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'clickstream_events') }}
)

SELECT
    event_id,
    user_id,
    session_id::UUID                                AS session_id,
    event_type,
    product_id,
    product_price::NUMERIC(10, 2)                   AS product_price,
    device_type,
    timestamp::TIMESTAMPTZ                          AS event_timestamp,
    EXTRACT(HOUR FROM timestamp)::INT               AS hour_of_day,
    EXTRACT(ISODOW FROM timestamp)::INT             AS day_of_week
FROM source
