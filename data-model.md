# Data Model

Entity definitions for the e-commerce source datasets and their relationships. Schemas apply to CSV source files, Bronze Delta tables, and Silver validated tables (Silver adds DQ columns).

---

## Entity Relationship

```text
┌──────────────────┐         ┌──────────────────┐
│    customers     │         │     products     │
│  PK: customer_id │         │  PK: product_id  │
└────────┬─────────┘         └────────┬─────────┘
         │                            │
         │ 1                          │ 1
         │                            │
         ▼ *                          ▼ *
┌────────────────────────────────────────────────┐
│                    orders                      │
│  PK: order_id                                  │
│  FK: customer_id → customers.customer_id       │
│  FK: product_id  → products.product_id         │
└────────────────────────────────────────────────┘
```

---

## customers

Master table for e-commerce customers.

| Column | Data Type | Nullable | Key | Description |
|--------|-----------|----------|-----|-------------|
| `customer_id` | INTEGER | NO | **PK** | Unique customer identifier |
| `first_name` | STRING | NO | | Customer first name |
| `last_name` | STRING | NO | | Customer last name |
| `email` | STRING | YES | | Contact email (completeness rule: required in Silver) |
| `signup_date` | DATE | NO | | Account registration date |
| `country` | STRING | NO | | Country code or name |

**Primary key:** `customer_id`

**Business rules (Silver):**

- `customer_id` must not be NULL
- `email` must not be NULL (completeness)
- `customer_id` must be unique

---

## products

Master table for sellable products.

| Column | Data Type | Nullable | Key | Description |
|--------|-----------|----------|-----|-------------|
| `product_id` | INTEGER | NO | **PK** | Unique product identifier |
| `product_name` | STRING | NO | | Product display name |
| `category` | STRING | NO | | Product category (e.g., Electronics, Clothing) |
| `price` | DECIMAL(10,2) | NO | | Unit price in USD |
| `created_date` | DATE | NO | | Product catalog entry date |

**Primary key:** `product_id`

**Business rules (Silver):**

- `product_id` must not be NULL and must be unique
- `price` must be greater than 0

---

## orders

Transactional order line items (one row per order line).

| Column | Data Type | Nullable | Key | Description |
|--------|-----------|----------|-----|-------------|
| `order_id` | INTEGER | NO | **PK** | Unique order identifier |
| `customer_id` | INTEGER | YES | **FK** → `customers.customer_id` | Ordering customer |
| `product_id` | INTEGER | YES | **FK** → `products.product_id` | Ordered product |
| `quantity` | INTEGER | NO | | Units ordered (≥ 1) |
| `order_date` | DATE | NO | | Date order was placed |
| `order_status` | STRING | NO | | Status (e.g., completed, pending, cancelled) |

**Primary key:** `order_id`

**Foreign keys:**

- `customer_id` references `customers.customer_id`
- `product_id` references `products.product_id`

**Business rules (Silver):**

- `order_id`, `customer_id`, `product_id` must not be NULL (completeness)
- `order_id` must be unique
- `customer_id` must exist in `customers`
- `product_id` must exist in `products`
- `quantity` must be ≥ 1

---

## Silver Layer — Additional Columns

Applied to all Silver entity tables during validation:

| Column | Data Type | Description |
|--------|-----------|-------------|
| `is_valid` | BOOLEAN | `true` if row passes all validation rules |
| `dq_failure_reasons` | STRING | Delimited list of failure reason codes |
| `_silver_processed_at` | TIMESTAMP | When Silver validation ran |

---

## Gold Layer — Planned Tables

| Table | Grain | Key columns |
|-------|-------|-------------|
| `gold_sales_by_product` | One row per product | `product_id` |
| `gold_revenue_by_customer` | One row per customer | `customer_id` |
| `gold_customer_segmentation` | One row per customer | `customer_id`, `segment` |

Detailed Gold column lists will be added during the Gold implementation phase.

---

## Revenue Calculation

```text
line_revenue = orders.quantity × products.price
```

- Price sourced from **valid** `silver_products` at aggregation time
- Only **valid** `silver_orders` included in Gold calculations
- Orders with invalid or missing product references are excluded via Silver flagging

---

## Identifier Ranges (Planned for Data Generation)

| Entity | Valid ID range (planned) | Notes |
|--------|--------------------------|-------|
| customers | 1 – 10,000 | 10 duplicate IDs will reuse existing IDs |
| products | 1 – 500 | Invalid refs use IDs outside this range |
| orders | 1 – 100,000 | 20 duplicate IDs will reuse existing IDs |

Exact generation logic documented in `src/data_generation/DATA_GENERATION_NOTES.md` and `database/seed-data-notes.md`.
