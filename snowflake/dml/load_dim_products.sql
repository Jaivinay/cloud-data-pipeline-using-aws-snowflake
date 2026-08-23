MERGE INTO dim_products AS target
USING (
    SELECT
        $1:product_id::VARCHAR    AS product_id,
        $1:product_name::VARCHAR  AS product_name,
        $1:category::VARCHAR      AS category,
        $1:unit_price::DOUBLE     AS unit_price
    FROM @processed_zone_stage/dim_products/
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    product_name = source.product_name, category = source.category, unit_price = source.unit_price
WHEN NOT MATCHED THEN INSERT (product_id, product_name, category, unit_price)
    VALUES (source.product_id, source.product_name, source.category, source.unit_price);
