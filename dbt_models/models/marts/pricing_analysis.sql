WITH products AS (
    SELECT * FROM {{ source('ecommerce', 'products') }}
),

competitor AS (
    SELECT
        product_id,
        ROUND(AVG(competitor_price)::NUMERIC, 2)   AS avg_competitor_price,
        ROUND(MIN(competitor_price)::NUMERIC, 2)   AS min_competitor_price,
        ROUND(MAX(competitor_price)::NUMERIC, 2)   AS max_competitor_price,
        COUNT(DISTINCT competitor_name)             AS competitor_count
    FROM {{ source('ecommerce', 'competitor_prices') }}
    WHERE timestamp > NOW() - INTERVAL '24 hours'
    GROUP BY product_id
),

latest_recs AS (
    SELECT DISTINCT ON (product_id)
        product_id,
        recommendation,
        confidence_score,
        recommended_price,
        timestamp AS rec_timestamp
    FROM {{ source('ecommerce', 'pricing_recommendations') }}
    ORDER BY product_id, timestamp DESC
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.base_price                                                           AS our_price,
    c.avg_competitor_price,
    c.min_competitor_price,
    c.max_competitor_price,
    c.competitor_count,
    CASE
        WHEN p.base_price < c.min_competitor_price * 0.95 THEN 'cheaper'
        WHEN p.base_price > c.max_competitor_price * 1.05 THEN 'pricier'
        ELSE 'competitive'
    END                                                                    AS price_position,
    r.recommendation,
    r.confidence_score,
    r.recommended_price,
    r.rec_timestamp
FROM products p
LEFT JOIN competitor   c ON p.product_id = c.product_id
LEFT JOIN latest_recs  r ON p.product_id = r.product_id
ORDER BY p.category, p.product_name
