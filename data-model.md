# Data Model

Entity definitions for CSV source files, Bronze Delta tables, Silver validated tables, and Gold aggregations.

**Source CSV schemas** match `src/data_generation/generate_sample_data.py` (Phase 1). Bronze tables add ingestion metadata columns documented below.

---

## Entity Relationship

```text
┌──────────────────┐         ┌──────────────────┐
│    customers     │         │     products     │
│  PK: customer_id │         │  PK: product_id  │
└────────┬─────────┘         └────────┬─────────┘
         │                            │
         │ 1                          │ 1
         ▼ *                          ▼ *
┌────────────────────────────────────────────────┐
│                    orders                      │
│  PK: order_id                                  │
│  FK: customer_id → customers.customer_id       │
│  FK: product_id  → products.product_id         │
└────────────────────────────────────────────────┘
```

---

## customers (CSV + Bronze)

| Column | Data Type | Nullable | Key | Description |
|--------|-----------|----------|-----|-------------|
| `customer_id` | INTEGER | NO | **PK** | Customer identifier (duplicates exist in source) |
| `customer_name` | STRING | NO | | Full customer name |
| `email` | STRING | YES | | Contact email (50 NULLs in sample data) |
| `country` | STRING | NO | | Country name |
| `signup_date` | DATE | NO | | Account registration date |
| `customer_segment` | STRING | NO | | Premium / Standard / Basic |
| `lifetime_value` | DECIMAL(12,2) | NO | | Lifetime value in USD |

**Primary key (logical):** `customer_id` — not unique in source due to intentional duplicates.

**Silver rules (implemented):** completeness, type/business foundation, uniqueness, and referential integrity. See `data-quality-strategy.md` for reason codes.

---

## products (CSV + Bronze)

| Column | Data Type | Nullable | Key | Description |
|--------|-----------|----------|-----|-------------|
| `product_id` | INTEGER | NO | **PK** | Unique product identifier |
| `product_name` | STRING | NO | | Product display name |
| `category` | STRING | NO | | Product category |
| `price` | DECIMAL(10,2) | NO | | Unit price USD |
| `cost` | DECIMAL(10,2) | NO | | Unit cost USD |
| `stock_quantity` | INTEGER | NO | | Units in stock |
| `reorder_level` | INTEGER | NO | | Reorder threshold |

**Primary key:** `product_id`

---

## orders (CSV + Bronze)

| Column | Data Type | Nullable | Key | Description |
|--------|-----------|----------|-----|-------------|
| `order_id` | INTEGER | NO | **PK** | Order identifier (duplicates in source) |
| `customer_id` | STRING | YES | **FK** | Raw CSV FK value (may be `8952` or `8952.0`; NULL in source) |
| `order_date` | DATE | NO | | Order placement date |
| `product_id` | STRING | YES | **FK** | Raw CSV FK value; NULL in source |
| `quantity` | INTEGER | NO | | Units ordered (≥ 1 in sample data) |
| `unit_price` | DECIMAL(10,2) | NO | | Price per unit |
| `total_amount` | DECIMAL(12,2) | NO | | `quantity × unit_price` in sample data |
| `order_status` | STRING | NO | | Pending / Completed / Cancelled |
| `payment_date` | DATE | YES | | Payment date when applicable |

**Foreign keys:** `customer_id`, `product_id` — nullable and may be invalid in source.

---

## Bronze Layer — Metadata Columns

Added to `bronze_customers`, `bronze_orders`, `bronze_products` during ingestion:

| Column | Data Type | Description |
|--------|-----------|-------------|
| `_ingestion_timestamp` | TIMESTAMP | UTC timestamp when Bronze ingestion ran |
| `_source_file` | STRING | Source CSV filename (e.g. `customers.csv`) |
| `_batch_id` | STRING | Pipeline run identifier shared across entities |

Bronze performs **no business cleaning** — all source rows and DQ issues are preserved.

### Bronze vs Silver type representation

| Column | Bronze (raw) | Silver (implemented foundation) |
|--------|--------------|----------------------------------|
| `orders.customer_id` | STRING (nullable) | INTEGER after parse/normalize |
| `orders.product_id` | STRING (nullable) | INTEGER after parse/normalize |
| Other source columns | Typed per CSV contract | Same types after validation |

**Silver FK normalization (implemented):**

- NULL / blank STRING FK → completeness failure (`NULL_CUSTOMER_ID`, `NULL_PRODUCT_ID`)
- Parse numeric strings (`8952`, `8952.0`) → integer
- Reject non-numeric garbage (e.g. `ABC`, `12.5`) with `INVALID_*_ID_TYPE` — not silent NULL
- Referential integrity checks on parsed integers — **implemented** (`04_quality_referential_integrity.py`)
- Raw Bronze STRING FK values retained in `bronze_orders` (no duplicate raw columns in Silver)

---

## bronze_ingestion_log

| Column | Data Type | Description |
|--------|-----------|-------------|
| `entity_name` | STRING | customers / orders / products |
| `source_file` | STRING | CSV filename |
| `row_count` | INTEGER | Rows ingested |
| `ingestion_timestamp` | TIMESTAMP | UTC ingestion time |
| `batch_id` | STRING | Pipeline run id |
| `status` | STRING | SUCCESS (failure stops pipeline before log write) |

---

## Silver Layer — Additional Columns (implemented foundation)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `dq_status` | STRING | `PASS` or `FAIL` |
| `dq_failure_reasons` | ARRAY&lt;STRING&gt; | Stable reason codes; empty when PASS |
| `_silver_processed_at` | TIMESTAMP | Silver validation timestamp |

Bronze metadata columns (`_ingestion_timestamp`, `_source_file`, `_batch_id`) flow through Silver when processing Bronze DataFrames.

### Silver tables

| Table | Write mode | Description |
|-------|------------|-------------|
| `silver_customers` | overwrite | Validated customers + DQ flags |
| `silver_products` | overwrite | Validated products + DQ flags |
| `silver_orders` | overwrite | Validated orders (INTEGER FKs) + DQ flags |
| `silver_dq_metrics` | append | Per-run RULE and OVERALL metrics |

### `silver_dq_metrics` schema

| Column | Type | Description |
|--------|------|-------------|
| `entity_name` | STRING | customers / orders / products |
| `metric_type` | STRING | `RULE` or `OVERALL` |
| `reason_code` | STRING | Reason code (null for OVERALL) |
| `rule_id` | STRING | Optional rule id (e.g. COMP-CUST-02) |
| `total_records` | INT | Entity row count |
| `failed_count` | INT | Failed rows for rule or OVERALL |
| `passed_count` | INT | Passed rows for rule or OVERALL |
| `failed_percentage` | DOUBLE | `failed_count / total_records × 100` |
| `passed_percentage` | DOUBLE | `passed_count / total_records × 100` |
| `batch_id` | STRING | Pipeline batch id |
| `metric_timestamp` | TIMESTAMP | When metrics were computed |

---

## Gold Layer Tables

| Table | Grain | Key columns |
|-------|-------|-------------|
| `gold_sales_by_product` | One row per product with business orders | `product_id`, `total_orders`, `total_revenue`, `avg_order_value` |
| `gold_revenue_by_customer` | One row per PASS customer | `customer_id`, `total_orders`, `total_revenue`, `lifetime_value_actual` |
| `gold_daily_weekly_trends` | One row per period | `period_type`, `period_start`, `total_orders`, `total_revenue` |
| `gold_customer_segmentation` | One row per segment | `segment_type`, `customer_count`, `avg_revenue`, `total_revenue` |

---

## Revenue Calculation (Gold)

Gold realized revenue uses Silver `orders.total_amount` for **trusted business orders**:

- `dq_status = 'PASS'` on order, customer, and product
- `order_status = 'Completed'`
- Inner join to PASS customer and PASS product

`lifetime_value_actual` on `gold_revenue_by_customer` is the sum of trusted business-order amounts per customer (observed). Source `customers.lifetime_value` is not used for Gold metrics.

---

## Dashboard Layer (Phase 5)

Dashboard SQL in `src/dashboard/` reads Gold tables only. No Bronze/Silver queries.

| Query file | Consumes | Output grain |
|------------|----------|--------------|
| `01_top_10_products_by_revenue.sql` | `gold_sales_by_product` | 10 products |
| `02_customer_revenue_distribution.sql` | `gold_revenue_by_customer` | Revenue bands |
| `03_customer_segmentation.sql` | `gold_customer_segmentation` | Segments |
| `04_revenue_trend.sql` | `gold_daily_weekly_trends` | Weekly periods |

---

## Identifier Ranges (Sample Data)

| Entity | ID range | Notes |
|--------|----------|-------|
| customers | 1 – 9,995 (+ duplicates for ids 1–5) | 10,000 rows |
| products | 1 – 500 | |
| orders | 1 – 99,990 | 100,000 rows; duplicate order ids 99601–99610 |

See `database/seed-data-notes.md` for intentional DQ issue counts.
