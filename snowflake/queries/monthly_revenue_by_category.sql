-- Monthly revenue by product category
SELECT
    d.year, d.month, p.category,
    SUM(f.line_total) AS revenue,
    COUNT(DISTINCT f.order_id) AS order_count
FROM fact_orders f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY d.year, d.month, p.category
ORDER BY d.year, d.month, revenue DESC;
