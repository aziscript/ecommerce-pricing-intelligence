WITH products AS (
    SELECT * FROM {{ source('ecommerce', 'products') }}
),

metrics AS (
    SELECT * FROM {{ source('ecommerce', 'product_metrics') }}
),

inventory AS (
    SELECT
        product_id,
        SUM(current_stock) AS total_stock
    FROM {{ source('ecommerce', 'inventory_state') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.base_price,
    COALESCE(m.total_views,     0)                                            AS total_views,
    COALESCE(m.total_cart_adds, 0)                                            AS total_cart_adds,
    COALESCE(m.total_purchases, 0)                                            AS total_purchases,
    COALESCE(m.revenue,         0)                                            AS total_revenue,
    ROUND(
        m.revenue::NUMERIC / NULLIF(m.total_purchases, 0),
        2
    )                                                                         AS avg_sale_price,
    COALESCE(m.conversion_rate, 0)                                            AS conversion_rate,
    COALESCE(i.total_stock,     0)                                            AS total_stock_all_warehouses,
    m.last_updated
FROM products p
LEFT JOIN metrics   m ON p.product_id = m.product_id
LEFT JOIN inventory i ON p.product_id = i.product_id
ORDER BY total_revenue DESC NULLS LAST
