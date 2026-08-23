-- Top 20 customers by total spend
SELECT
    c.customer_id, c.first_name, c.last_name, c.state,
    SUM(f.line_total) AS lifetime_value,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.line_total) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.state
ORDER BY lifetime_value DESC
LIMIT 20;
