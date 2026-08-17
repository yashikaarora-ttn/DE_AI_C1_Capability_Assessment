-- Gold: customer segmentation (mutually exclusive segments)
-- Population: all PASS Silver customers.
-- Metrics: trusted business orders only.
-- HIGH_VALUE_THRESHOLD: configurable (default 1000 in PySpark; substitute in SQL).

WITH customer_metrics AS (
    SELECT
        c.customer_id,
        COALESCE(stats.total_orders, 0) AS total_orders,
        COALESCE(stats.total_revenue, 0) AS total_revenue
    FROM ${schema}.silver_customers c
    LEFT JOIN (
        SELECT
            o.customer_id,
            COUNT(*) AS total_orders,
            SUM(o.total_amount) AS total_revenue
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
    WHERE c.dq_status = 'PASS'
),
segmented AS (
    SELECT
        customer_id,
        total_orders,
        total_revenue,
        CASE
            WHEN total_orders = 0 THEN 'Inactive'
            WHEN total_orders = 1 THEN 'One-Time'
            WHEN total_orders > 1 AND total_revenue >= ${high_value_threshold} THEN 'High-Value'
            WHEN total_orders > 1 THEN 'Repeat'
            ELSE 'Inactive'
        END AS segment_type
    FROM customer_metrics
)
SELECT
    segment_type,
    COUNT(*) AS customer_count,
    AVG(total_revenue) AS avg_revenue,
    SUM(total_revenue) AS total_revenue
FROM segmented
GROUP BY segment_type;
