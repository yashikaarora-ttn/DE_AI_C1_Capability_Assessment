-- Dashboard: Customer Segmentation
-- Source: gold_customer_segmentation (Gold layer only)
-- Segmentation rules are defined in Gold — do not reimplement here.

SELECT
    segment_type,
    customer_count,
    avg_revenue,
    total_revenue
FROM ${schema}.gold_customer_segmentation
ORDER BY
    CASE segment_type
        WHEN 'Inactive' THEN 1
        WHEN 'One-Time' THEN 2
        WHEN 'Repeat' THEN 3
        WHEN 'High-Value' THEN 4
        ELSE 5
    END;
