-- Gold: daily and weekly business trends
-- Trusted policy: trusted business orders (PASS Completed + PASS customer + PASS product).

WITH trusted_business AS (
    SELECT
        o.order_date,
        o.total_amount
    FROM ${schema}.silver_orders o
    INNER JOIN ${schema}.silver_customers c
        ON o.customer_id = c.customer_id
    INNER JOIN ${schema}.silver_products p
        ON o.product_id = p.product_id
    WHERE o.dq_status = 'PASS'
      AND c.dq_status = 'PASS'
      AND p.dq_status = 'PASS'
      AND o.order_status = 'Completed'
),
daily AS (
    SELECT
        'DAILY' AS period_type,
        order_date AS period_start,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM trusted_business
    GROUP BY order_date
),
weekly AS (
    SELECT
        'WEEKLY' AS period_type,
        DATE_TRUNC('week', order_date) AS period_start,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM trusted_business
    GROUP BY DATE_TRUNC('week', order_date)
)
SELECT
    period_type,
    period_start,
    total_orders,
    total_revenue,
    avg_order_value
FROM daily
UNION ALL
SELECT
    period_type,
    period_start,
    total_orders,
    total_revenue,
    avg_order_value
FROM weekly;
