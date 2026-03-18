WITH clickstream AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
),

products AS (
    SELECT product_id, category
    FROM {{ source('ecommerce', 'products') }}
)

SELECT
    DATE_TRUNC('hour', c.event_timestamp)                                    AS hour_bucket,
    p.category,
    COUNT(*) FILTER (WHERE c.event_type = 'purchase')                        AS purchases,
    COUNT(*) FILTER (WHERE c.event_type = 'product_view')                    AS product_views,
    COUNT(*) FILTER (WHERE c.event_type = 'add_to_cart')                     AS cart_adds
FROM clickstream c
JOIN products p ON c.product_id = p.product_id
WHERE c.event_type IN ('purchase', 'product_view', 'add_to_cart')
  AND c.product_id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 2
