-- Gold: sales by product
-- Trusted policy: PASS Silver orders + PASS customers + PASS products; Completed only.
-- Inner joins exclude completed PASS orders whose customer or product failed Silver.

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(*) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM ${schema}.silver_orders o
INNER JOIN ${schema}.silver_customers c
    ON o.customer_id = c.customer_id
INNER JOIN ${schema}.silver_products p
    ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS'
  AND c.dq_status = 'PASS'
  AND p.dq_status = 'PASS'
  AND o.order_status = 'Completed'
GROUP BY
    p.product_id,
    p.product_name,
    p.category;
