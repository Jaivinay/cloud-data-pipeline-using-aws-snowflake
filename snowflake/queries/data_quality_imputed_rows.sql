-- Surfaces how much of fact_orders relied on the quantity imputation the ETL job does
-- for null quantities (see clean_orders() in src/utils/transform.py) - a BI consumer
-- of this table should be able to see this, not just trust it silently.
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN quantity_was_imputed THEN 1 ELSE 0 END) AS imputed_rows,
    ROUND(100.0 * SUM(CASE WHEN quantity_was_imputed THEN 1 ELSE 0 END) / COUNT(*), 2) AS imputed_pct
FROM fact_orders;
