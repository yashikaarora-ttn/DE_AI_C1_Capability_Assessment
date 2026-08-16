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

**Silver rules (planned):** completeness, uniqueness, email not null.

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

| Column | Bronze (raw) | Silver (planned) |
|--------|--------------|------------------|
| `orders.customer_id` | STRING (nullable) | INTEGER after parse/normalize |
| `orders.product_id` | STRING (nullable) | INTEGER after parse/normalize |
| Other source columns | Typed per CSV contract | Same types after validation |

Bronze stores nullable order FKs as STRING because pandas CSV may emit values like
`8952.0`. Silver will normalize (`8952.0` → `8952`), validate, and flag unparseable values.

**Silver FK normalization (planned):**

- NULL / empty → completeness failure
- Parse numeric strings (`8952`, `8952.0`) → integer
- Reject non-numeric garbage (e.g. `ABC`, empty after trim)
- Referential integrity checks on parsed integers

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

## Silver Layer — Additional Columns (planned)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `is_valid` | BOOLEAN | Passes all Silver validation rules |
| `dq_failure_reasons` | STRING | Delimited failure reason codes |
| `_silver_processed_at` | TIMESTAMP | Silver validation timestamp |

---

## Gold Layer — Planned Tables

| Table | Grain | Key columns |
|-------|-------|-------------|
| `gold_sales_by_product` | One row per product | `product_id` |
| `gold_revenue_by_customer` | One row per customer | `customer_id` |
| `gold_customer_segmentation` | One row per customer | `customer_id`, `segment` |

---

## Revenue Calculation (Gold)

```text
line_revenue = orders.quantity × products.price
```

Gold will use validated Silver data only.

---

## Identifier Ranges (Sample Data)

| Entity | ID range | Notes |
|--------|----------|-------|
| customers | 1 – 9,995 (+ duplicates for ids 1–5) | 10,000 rows |
| products | 1 – 500 | |
| orders | 1 – 99,990 | 100,000 rows; duplicate order ids 99601–99610 |

See `database/seed-data-notes.md` for intentional DQ issue counts.
