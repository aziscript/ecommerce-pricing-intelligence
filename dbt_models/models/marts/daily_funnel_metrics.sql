WITH clickstream AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
),

products AS (
    SELECT product_id, category
    FROM {{ source('ecommerce', 'products') }}
)

SELECT
    event_timestamp::DATE                                                           AS day,
    COALESCE(p.category, 'unknown')                                                AS category,
    COUNT(DISTINCT CASE WHEN c.event_type = 'page_view'    THEN c.session_id END)  AS page_views,
    COUNT(DISTINCT CASE WHEN c.event_type = 'product_view' THEN c.session_id END)  AS product_views,
    COUNT(DISTINCT CASE WHEN c.event_type = 'add_to_cart'  THEN c.session_id END)  AS cart_adds,
    COUNT(DISTINCT CASE WHEN c.event_type = 'purchase'     THEN c.session_id END)  AS purchases,
    ROUND(
        COUNT(DISTINCT CASE WHEN c.event_type = 'purchase' THEN c.session_id END)::NUMERIC
        / NULLIF(
            COUNT(DISTINCT CASE WHEN c.event_type = 'page_view' THEN c.session_id END), 0
          ) * 100,
        2
    )                                                                               AS conversion_rate
FROM clickstream c
LEFT JOIN products p ON c.product_id = p.product_id
GROUP BY 1, 2
ORDER BY 1 DESC, 2
