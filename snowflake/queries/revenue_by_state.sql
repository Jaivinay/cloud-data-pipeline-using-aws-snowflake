-- Revenue and order count by customer state - useful for a quick regional breakdown
SELECT
    c.state,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.line_total) AS revenue,
    ROUND(SUM(f.line_total) / COUNT(DISTINCT c.customer_id), 2) AS revenue_per_customer
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.state
ORDER BY revenue DESC;
