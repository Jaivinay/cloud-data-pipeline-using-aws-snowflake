-- MERGE, not INSERT: pipeline re-runs shouldn't create duplicate customer rows in the
-- warehouse, and existing customers' data should update if the source record changed.
MERGE INTO dim_customers AS target
USING (
    SELECT
        $1:customer_id::VARCHAR      AS customer_id,
        $1:first_name::VARCHAR       AS first_name,
        $1:last_name::VARCHAR        AS last_name,
        $1:email::VARCHAR            AS email,
        $1:state::VARCHAR            AS state,
        $1:signup_date::DATE         AS signup_date,
        $1:has_valid_email::BOOLEAN  AS has_valid_email
    FROM @processed_zone_stage/dim_customers/
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    first_name = source.first_name, last_name = source.last_name, email = source.email,
    state = source.state, signup_date = source.signup_date, has_valid_email = source.has_valid_email
WHEN NOT MATCHED THEN INSERT (customer_id, first_name, last_name, email, state, signup_date, has_valid_email)
    VALUES (source.customer_id, source.first_name, source.last_name, source.email,
            source.state, source.signup_date, source.has_valid_email);
