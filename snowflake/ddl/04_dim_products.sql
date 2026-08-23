CREATE TABLE IF NOT EXISTS dim_products (
    product_id    VARCHAR(20) PRIMARY KEY,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    unit_price    DOUBLE,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
