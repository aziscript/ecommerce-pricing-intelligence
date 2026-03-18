-- =============================================================================
-- init.sql  —  E-commerce Pricing Intelligence Platform
-- Schema: ecommerce
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Bootstrap
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce;

-- -----------------------------------------------------------------------------
-- 1. products — canonical product catalog
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id   VARCHAR(10)    PRIMARY KEY,
    product_name VARCHAR(255)   NOT NULL,
    category     VARCHAR(50)    NOT NULL,
    base_price   NUMERIC(10, 2) NOT NULL CHECK (base_price > 0),
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 2. inventory_state — real-time stock per product per warehouse
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_state (
    product_id     VARCHAR(10)  NOT NULL REFERENCES products(product_id),
    warehouse_id   VARCHAR(20)  NOT NULL,
    current_stock  INTEGER      NOT NULL DEFAULT 0 CHECK (current_stock >= 0),
    last_updated   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, warehouse_id)
);

-- -----------------------------------------------------------------------------
-- 3. clickstream_events — raw clickstream for batch analytics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream_events (
    event_id    UUID           PRIMARY KEY,
    user_id     VARCHAR(20)    NOT NULL,
    session_id  UUID           NOT NULL,
    event_type  VARCHAR(30)    NOT NULL,
    product_id  VARCHAR(10)    REFERENCES products(product_id),
    product_price NUMERIC(10, 2),
    device_type VARCHAR(10)    NOT NULL,
    timestamp   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clickstream_product_id
    ON clickstream_events(product_id);
CREATE INDEX IF NOT EXISTS idx_clickstream_timestamp
    ON clickstream_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_clickstream_event_type
    ON clickstream_events(event_type);
CREATE INDEX IF NOT EXISTS idx_clickstream_user_session
    ON clickstream_events(user_id, session_id);

-- -----------------------------------------------------------------------------
-- 4. inventory_events — raw inventory audit log
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_events (
    event_id        UUID           PRIMARY KEY,
    product_id      VARCHAR(10)    NOT NULL REFERENCES products(product_id),
    warehouse_id    VARCHAR(20)    NOT NULL,
    event_type      VARCHAR(30)    NOT NULL,
    quantity_change INTEGER        NOT NULL,
    timestamp       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inventory_events_product_id
    ON inventory_events(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_events_timestamp
    ON inventory_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_inventory_events_warehouse
    ON inventory_events(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_inventory_events_event_type
    ON inventory_events(event_type);

-- -----------------------------------------------------------------------------
-- 5. competitor_prices — latest observed competitor price per product/competitor
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS competitor_prices (
    id                   SERIAL         PRIMARY KEY,
    product_id           VARCHAR(10)    NOT NULL REFERENCES products(product_id),
    competitor_name      VARCHAR(50)    NOT NULL,
    competitor_price     NUMERIC(10, 2) NOT NULL,
    our_price            NUMERIC(10, 2) NOT NULL,
    price_difference     NUMERIC(10, 2) NOT NULL,
    price_difference_pct NUMERIC(6, 2)  NOT NULL,
    timestamp            TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_competitor_prices_product_id
    ON competitor_prices(product_id);
CREATE INDEX IF NOT EXISTS idx_competitor_prices_timestamp
    ON competitor_prices(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_competitor_prices_competitor
    ON competitor_prices(competitor_name);

-- -----------------------------------------------------------------------------
-- 6. pricing_recommendations — output of the pricing engine
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pricing_recommendations (
    id                   SERIAL         PRIMARY KEY,
    product_id           VARCHAR(10)    NOT NULL REFERENCES products(product_id),
    product_name         VARCHAR(255)   NOT NULL,
    current_price        NUMERIC(10, 2) NOT NULL,
    avg_competitor_price NUMERIC(10, 2) NOT NULL,
    demand_velocity      NUMERIC(10, 4) NOT NULL,  -- events per minute
    recommendation       VARCHAR(10)    NOT NULL CHECK (recommendation IN ('raise', 'lower', 'hold')),
    confidence_score     NUMERIC(4, 3)  NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    recommended_price    NUMERIC(10, 2) NOT NULL,
    timestamp            TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pricing_rec_product_id
    ON pricing_recommendations(product_id);
CREATE INDEX IF NOT EXISTS idx_pricing_rec_timestamp
    ON pricing_recommendations(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pricing_rec_recommendation
    ON pricing_recommendations(recommendation);

-- -----------------------------------------------------------------------------
-- 7. product_metrics — rolling KPIs per product (upserted by stream processor)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_metrics (
    product_id       VARCHAR(10)    PRIMARY KEY REFERENCES products(product_id),
    total_views      BIGINT         NOT NULL DEFAULT 0,
    total_cart_adds  BIGINT         NOT NULL DEFAULT 0,
    total_purchases  BIGINT         NOT NULL DEFAULT 0,
    conversion_rate  NUMERIC(6, 4)  NOT NULL DEFAULT 0, -- purchases / views
    revenue          NUMERIC(14, 2) NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Seed: 30 products (mirrors generators/catalog.py)
-- -----------------------------------------------------------------------------
INSERT INTO products (product_id, product_name, category, base_price) VALUES
    -- Phones
    ('P001', 'Samsung Galaxy S24',             'phones',      899.99),
    ('P002', 'iPhone 15 Pro',                  'phones',     1199.99),
    ('P003', 'Google Pixel 8',                 'phones',      699.99),
    ('P004', 'OnePlus 12',                     'phones',      799.99),
    ('P005', 'Xiaomi 14 Pro',                  'phones',      649.99),
    ('P006', 'Motorola Edge 40',               'phones',      349.99),
    -- Laptops
    ('P007', 'MacBook Pro 16-inch',            'laptops',    1999.99),
    ('P008', 'Dell XPS 15',                    'laptops',    1799.99),
    ('P009', 'Lenovo ThinkPad X1 Carbon',      'laptops',    1599.99),
    ('P010', 'ASUS ROG Zephyrus G14',          'laptops',    1399.99),
    ('P011', 'HP Spectre x360',                'laptops',    1299.99),
    ('P012', 'Acer Swift 3',                   'laptops',     649.99),
    -- Headphones
    ('P013', 'Sony WH-1000XM5',               'headphones',   349.99),
    ('P014', 'Apple AirPods Pro 2',            'headphones',   249.99),
    ('P015', 'Bose QuietComfort 45',           'headphones',   329.99),
    ('P016', 'Sennheiser Momentum 4',          'headphones',   299.99),
    ('P017', 'Jabra Evolve2 85',               'headphones',   379.99),
    ('P018', 'Anker Soundcore Q45',            'headphones',    59.99),
    -- Tablets
    ('P019', 'iPad Pro 12.9-inch',             'tablets',    1099.99),
    ('P020', 'Samsung Galaxy Tab S9',          'tablets',     799.99),
    ('P021', 'Microsoft Surface Pro 9',        'tablets',    1299.99),
    ('P022', 'Amazon Fire HD 10',              'tablets',     149.99),
    ('P023', 'Lenovo Tab P12 Pro',             'tablets',     549.99),
    ('P024', 'Xiaomi Pad 6 Pro',               'tablets',     399.99),
    -- Accessories
    ('P025', 'Anker USB-C Hub 7-in-1',         'accessories',  49.99),
    ('P026', 'Samsung 65W USB-C Charger',      'accessories',  39.99),
    ('P027', 'Apple MagSafe Charger',          'accessories',  39.99),
    ('P028', 'Logitech MX Master 3S',          'accessories',  99.99),
    ('P029', 'SanDisk 1TB Portable SSD',       'accessories', 129.99),
    ('P030', 'Belkin 3-in-1 Wireless Charger', 'accessories',  79.99)
ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        category     = EXCLUDED.category,
        base_price   = EXCLUDED.base_price;

-- Seed inventory_state — zero stock in each warehouse for every product
INSERT INTO inventory_state (product_id, warehouse_id, current_stock)
SELECT p.product_id, w.warehouse_id, 0
FROM   products p
CROSS JOIN (VALUES ('WH-LAGOS'), ('WH-ABUJA'), ('WH-PH')) AS w(warehouse_id)
ON CONFLICT (product_id, warehouse_id) DO NOTHING;

-- Seed product_metrics — zero-row per product so upserts never need an INSERT
INSERT INTO product_metrics (product_id)
SELECT product_id FROM products
ON CONFLICT (product_id) DO NOTHING;
