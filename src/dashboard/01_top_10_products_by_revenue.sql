-- Dashboard: Top 10 Products by Revenue
-- Source: gold_sales_by_product (Gold layer only — do not query Silver/Bronze)
-- Replace ${schema} with your catalog.schema (e.g. main.ecommerce_medallion)

SELECT
    product_id,
    product_name,
    category,
    total_orders,
    total_revenue,
    avg_order_value
FROM ${schema}.gold_sales_by_product
ORDER BY total_revenue DESC, product_id ASC
LIMIT 10;
