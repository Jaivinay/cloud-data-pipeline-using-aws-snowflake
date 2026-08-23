CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id      VARCHAR(20) PRIMARY KEY,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    email            VARCHAR(255),
    state            VARCHAR(2),
    signup_date      DATE,
    has_valid_email  BOOLEAN,
    loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
