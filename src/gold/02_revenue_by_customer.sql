-- Gold: revenue by customer
-- Trusted policy: PASS Silver customers; realized metrics from trusted business orders.
-- Business orders: PASS Completed orders joining PASS customer and PASS product.
-- All PASS customers appear; zero business orders yield zero revenue metrics.
-- lifetime_value_actual = observed trusted business-order revenue (not source lifetime_value).

SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    COALESCE(stats.total_orders, 0) AS total_orders,
    COALESCE(stats.total_revenue, 0) AS total_revenue,
    COALESCE(stats.avg_order_value, 0) AS avg_order_value,
    COALESCE(stats.total_revenue, 0) AS lifetime_value_actual
FROM ${schema}.silver_customers c
LEFT JOIN (
    SELECT
        o.customer_id,
        COUNT(*) AS total_orders,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value
    FROM ${schema}.silver_orders o
    INNER JOIN ${schema}.silver_customers c2
        ON o.customer_id = c2.customer_id
    INNER JOIN ${schema}.silver_products p
        ON o.product_id = p.product_id
    WHERE o.dq_status = 'PASS'
      AND c2.dq_status = 'PASS'
      AND p.dq_status = 'PASS'
      AND o.order_status = 'Completed'
    GROUP BY o.customer_id
) stats
    ON c.customer_id = stats.customer_id
WHERE c.dq_status = 'PASS';
