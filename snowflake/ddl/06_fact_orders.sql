CREATE TABLE IF NOT EXISTS fact_orders (
    order_id              VARCHAR(20) PRIMARY KEY,
    customer_id           VARCHAR(20) REFERENCES dim_customers(customer_id),
    product_id            VARCHAR(20) REFERENCES dim_products(product_id),
    date_key              INT REFERENCES dim_date(date_key),
    quantity               INT,
    unit_price_at_order    DOUBLE,
    line_total             DOUBLE,
    quantity_was_imputed   BOOLEAN,
    loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Snowflake doesn't enforce FK constraints (they're metadata-only, used by the query
-- optimizer and BI tools for join inference) - worth knowing if you're used to Postgres/
-- MySQL where these are actually enforced. Data quality has to be enforced upstream,
-- which is exactly what src/utils/data_quality.py does before this table ever gets loaded.
