# Seed Data Notes

Sample e-commerce CSV datasets produced by `src/data_generation/generate_sample_data.py` (Phase 1).

---

## Overview

| File | Rows | Primary key | Output path |
|------|------|-------------|-------------|
| `customers.csv` | 10,000 | `customer_id` | `data/customers.csv` |
| `orders.csv` | 100,000 | `order_id` | `data/orders.csv` |
| `products.csv` | 500 | `product_id` | `data/products.csv` |

---

## Generation

```bash
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
```

Generation order: **customers → products → orders** (masters before transactions).

---

## Duplicate Row Definition

A **duplicate row** is any row whose primary key value appears more than once in the file. All rows in duplicated key groups are counted.

| Target | Implementation |
|--------|----------------|
| 10 duplicate `customer_id` rows | `customer_id` 1–5 each appear exactly twice |
| 20 duplicate `order_id` rows | 10 order ids each appear exactly twice |

---

## Intentional Data-Quality Issues

### Customers

| Issue | Count | Implementation |
|-------|-------|----------------|
| NULL `email` | 50 | Empty/NULL on rows with `customer_id` > 5 |
| Duplicate `customer_id` rows | 10 | ids 1–5 duplicated with different attributes |

### Orders (disjoint row sets)

| Issue | Count | Implementation |
|-------|-------|----------------|
| NULL `customer_id` | 100 | Dedicated rows |
| NULL `product_id` | 200 | Dedicated rows |
| Invalid `customer_id` | 50 | IDs 90001–90050 (non-null) |
| Invalid `product_id` | 30 | IDs 9001–9030 (non-null) |
| Duplicate `order_id` rows | 20 | 10 ids × 2 rows |

Clean valid orders: **99,600** rows.

NULL and invalid FK categories do **not** overlap. Duplicate order rows are separate from NULL/invalid issue rows.

---

## ID Ranges

| Entity | Valid range | Invalid reference range |
|--------|-------------|-------------------------|
| customers | 1 – 9,995 (+ duplicates for ids 1–5) | 90001–90050 for order FKs |
| products | 1 – 500 | 9001–9030 for order FKs |
| orders | 1 – 99,990 (100,000 rows; duplicate groups use ids 99601–99610) | Duplicate groups use dedicated id ranges |

---

## CSV Schema

### customers

`customer_id`, `customer_name`, `email`, `country`, `signup_date`, `customer_segment`, `lifetime_value`

### products

`product_id`, `product_name`, `category`, `price`, `cost`, `stock_quantity`, `reorder_level`

### orders

`order_id`, `customer_id`, `order_date`, `product_id`, `quantity`, `unit_price`, `total_amount`, `order_status`, `payment_date`

---

## File Format

| Setting | Value |
|---------|-------|
| Format | CSV |
| Header | Yes |
| Encoding | UTF-8 |
| Date format | `YYYY-MM-DD` |
| NULL representation | Empty field |

---

## Verification

Automated: `pytest tests/test_data_generation.py -v`

Manual checklist:

- [x] Row counts: 10,000 / 500 / 100,000
- [x] NULL email = 50
- [x] Duplicate customer_id rows = 10
- [x] NULL customer_id on orders = 100
- [x] NULL product_id on orders = 200
- [x] Invalid customer_id = 50
- [x] Invalid product_id = 30
- [x] Duplicate order_id rows = 20
- [x] Valid FKs on clean rows
- [x] `total_amount = quantity × unit_price`

---

## Status

| Task | Status |
|------|--------|
| Generator script | Done (`generate_sample_data.py`) |
| Verification tests | Done |
| CSV files produced | Run generator locally |

See `src/data_generation/DATA_GENERATION_NOTES.md` for module details.
