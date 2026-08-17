-- Dashboard: Weekly Revenue Trend
-- Source: gold_daily_weekly_trends (Gold layer only)
-- Uses pre-aggregated WEEKLY rows — do not recompute from orders.

SELECT
    period_start,
    total_orders,
    total_revenue,
    avg_order_value
FROM ${schema}.gold_daily_weekly_trends
WHERE period_type = 'WEEKLY'
ORDER BY period_start ASC;
