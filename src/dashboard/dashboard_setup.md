# Databricks SQL Dashboard Setup

Step-by-step guide for evaluators to build the assessment dashboard from Gold tables.

**Status:** SQL assets prepared locally. Databricks SQL Warehouse execution and dashboard rendering are **not yet validated** in this repository.

---

## Prerequisites

1. Gold tables populated in Unity Catalog (or Hive metastore):
   - `gold_sales_by_product`
   - `gold_revenue_by_customer`
   - `gold_customer_segmentation`
   - `gold_daily_weekly_trends`
2. Run Gold pipeline: `python src/gold/create_gold_tables.py` (on Databricks after Silver exists).
3. SQL Warehouse (Pro or Serverless) with access to the Gold schema.

---

## Step 1 — Open SQL Warehouse

1. In Databricks workspace, go to **SQL** → **SQL Warehouses**.
2. Start or select a warehouse (e.g. assessment cluster).
3. Open **SQL Editor** (or create a new query).

---

## Step 2 — Configure schema placeholder

Replace `${schema}` in each query file with your qualified schema, for example:

```text
main.ecommerce_medallion
```

Or set a query parameter / variable if your workflow supports it.

---

## Step 3 — Run each dashboard query

| File | Purpose | Recommended chart |
|------|---------|-------------------|
| `01_top_10_products_by_revenue.sql` | Top 10 products by realized revenue | **Bar chart** (horizontal or vertical) |
| `02_customer_revenue_distribution.sql` | Customer counts by revenue band | **Column/bar chart** |
| `03_customer_segmentation.sql` | Segment counts and revenue | **Pie/donut** or **bar chart** |
| `04_revenue_trend.sql` | Weekly revenue over time | **Line chart** |

For each file:

1. Paste the SQL into the SQL Editor (after replacing `${schema}`).
2. Run the query and confirm rows return.
3. Click **+** → **Visualization** (or Add visualization).
4. Choose the chart type from the table above.
5. Map axes:
   - **Top 10:** X = `product_name` (or `product_id`), Y = `total_revenue`
   - **Revenue distribution:** X = `revenue_band`, Y = `customer_count` (or `total_revenue`)
   - **Segmentation:** Label = `segment_type`, Value = `customer_count` or `total_revenue`
   - **Weekly trend:** X = `period_start`, Y = `total_revenue` (line); optional second series for `total_orders`

---

## Step 4 — Create the dashboard

1. Go to **Dashboards** → **Create dashboard**.
2. Name it (e.g. `E-commerce Medallion — Gold Metrics`).
3. Add each visualization from Step 3.
4. Arrange tiles and add titles.

---

## Step 5 — Optional filters (Gold-supported only)

These filters work cleanly because Gold tables expose the columns:

| Filter | Query | Gold column |
|--------|-------|-------------|
| Product category | `01_top_10_products_by_revenue.sql` | Add `WHERE category = ...` before `ORDER BY` (note: may return fewer than 10 rows) |
| Customer segment (source attribute) | Custom query on `gold_revenue_by_customer` | `customer_segment` — not included in default distribution query |
| Trend date range | `04_revenue_trend.sql` | Add `AND period_start BETWEEN ... AND ...` |

Do not add filters that require Silver/Bronze columns — dashboard SQL must stay Gold-only.

---

## Revenue distribution bands (presentation assumptions)

Configured in `02_customer_revenue_distribution.sql` for assessment-scale data (seed 42):

| Band | Rule (`total_revenue` from `gold_revenue_by_customer`) |
|------|--------------------------------------------------------|
| No Revenue | `= 0` |
| Low | `> 0` and `< 500` |
| Medium | `>= 500` and `< 2000` |
| High | `>= 2000` and `< 5000` |
| Very High | `>= 5000` |

These are **dashboard visualization thresholds**, not Gold segmentation rules. Gold High-Value segmentation uses `GOLD_HIGH_VALUE_THRESHOLD` (default 1000) with order-count logic — see Gold layer docs.

---

## Local validation

Static contract tests: `pytest tests/test_dashboard_queries.py -q`

These verify SQL file structure and Gold-only references; they do **not** prove Databricks execution.

---

## Troubleshooting

| Issue | Check |
|-------|-------|
| Table not found | Gold pipeline run; catalog/schema name in `${schema}` |
| Empty results | Silver/Gold pipeline completed; `gold_*` row counts |
| Visualization missing columns | Re-run query; confirm aliases match setup guide |
