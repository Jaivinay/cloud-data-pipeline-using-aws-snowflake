-- Load dimensions FIRST (see load_dim_*.sql) - this MERGE assumes customer_id/product_id/
-- date_key already exist in the dimension tables, since Snowflake FKs aren't enforced and
-- won't stop you from loading facts out of order, they just won't be joinable correctly.
MERGE INTO fact_orders AS target
USING (
    SELECT
        $1:order_id::VARCHAR              AS order_id,
        $1:customer_id::VARCHAR           AS customer_id,
        $1:product_id::VARCHAR            AS product_id,
        $1:date_key::INT                  AS date_key,
        $1:quantity::INT                  AS quantity,
        $1:unit_price_at_order::DOUBLE    AS unit_price_at_order,
        $1:line_total::DOUBLE             AS line_total,
        $1:quantity_was_imputed::BOOLEAN  AS quantity_was_imputed
    FROM @processed_zone_stage/fact_orders/
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    customer_id = source.customer_id, product_id = source.product_id, date_key = source.date_key,
    quantity = source.quantity, unit_price_at_order = source.unit_price_at_order,
    line_total = source.line_total, quantity_was_imputed = source.quantity_was_imputed
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, product_id, date_key, quantity,
                               unit_price_at_order, line_total, quantity_was_imputed)
    VALUES (source.order_id, source.customer_id, source.product_id, source.date_key,
            source.quantity, source.unit_price_at_order, source.line_total, source.quantity_was_imputed);
