WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'competitor_prices') }}
),

latest AS (
    SELECT DISTINCT ON (product_id, competitor_name)
        product_id,
        competitor_name,
        competitor_price::NUMERIC(10, 2)   AS competitor_price,
        our_price::NUMERIC(10, 2)          AS our_price,
        price_difference::NUMERIC(10, 2)   AS price_difference,
        price_difference_pct::NUMERIC(6, 2) AS price_difference_pct,
        timestamp::TIMESTAMPTZ             AS observed_at
    FROM source
    ORDER BY product_id, competitor_name, timestamp DESC
)

SELECT * FROM latest
