-- Dashboard: Customer Revenue Distribution
-- Source: gold_revenue_by_customer (Gold layer only)
--
-- Revenue bands are dashboard presentation assumptions for seed-42 scale data.
-- Adjust thresholds in CASE expressions if your Gold snapshot differs materially.
-- Not universal business rules.
--
-- Bands (mutually exclusive, every customer in exactly one):
--   No Revenue : total_revenue = 0
--   Low        : 0 < total_revenue < 500
--   Medium     : 500 <= total_revenue < 2000
--   High       : 2000 <= total_revenue < 5000
--   Very High  : total_revenue >= 5000

WITH banded_customers AS (
    SELECT
        customer_id,
        total_revenue,
        CASE
            WHEN total_revenue = 0 THEN 'No Revenue'
            WHEN total_revenue > 0 AND total_revenue < 500 THEN 'Low'
            WHEN total_revenue >= 500 AND total_revenue < 2000 THEN 'Medium'
            WHEN total_revenue >= 2000 AND total_revenue < 5000 THEN 'High'
            WHEN total_revenue >= 5000 THEN 'Very High'
        END AS revenue_band
    FROM ${schema}.gold_revenue_by_customer
)
SELECT
    revenue_band,
    COUNT(*) AS customer_count,
    SUM(total_revenue) AS total_revenue,
    AVG(total_revenue) AS avg_customer_revenue
FROM banded_customers
GROUP BY revenue_band
ORDER BY
    CASE revenue_band
        WHEN 'No Revenue' THEN 1
        WHEN 'Low' THEN 2
        WHEN 'Medium' THEN 3
        WHEN 'High' THEN 4
        WHEN 'Very High' THEN 5
    END;
