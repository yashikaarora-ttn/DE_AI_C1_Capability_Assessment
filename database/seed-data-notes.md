# Seed Data Notes

Plan for generating realistic e-commerce CSV datasets with intentional data-quality issues. **Data not yet generated** — Phase 1 implementation.

---

## Overview

Three CSV files will be produced by `src/data_generation/` (planned):

| File | Rows | Primary key |
|------|------|-------------|
| `customers.csv` | 10,000 | `customer_id` |
| `orders.csv` | 100,000 | `order_id` |
| `products.csv` | 500 | `product_id` |

Output location (planned): `data/raw/` (gitignored when generated).

---

## Generation Approach (Planned)

1. **Seeded randomness** — Fixed seed for reproducible datasets and tests
2. **Realistic values** — Faker or curated name/email/category lists
3. **Valid base population** — Generate clean data first
4. **Inject DQ issues** — Apply intentional defects at exact counts
5. **Validate counts** — `pytest` asserts row counts and defect counts before pipeline use

---

## Intentional Data-Quality Issues

| Dataset | Issue | Count | Implementation note |
|---------|-------|-------|---------------------|
| customers | NULL `email` | 50 | Set email to empty/NULL on random valid rows |
| customers | Duplicate `customer_id` | 10 | Reuse 10 existing IDs with different row content |
| orders | NULL `customer_id` | 100 | NULL out FK on random rows |
| orders | NULL `product_id` | 200 | NULL out FK on random rows |
| orders | Invalid `customer_id` | 50 | Use IDs not in customers (e.g., 90001–90050) |
| orders | Invalid `product_id` | 30 | Use IDs not in products (e.g., 9001–9030) |
| orders | Duplicate `order_id` | 20 | Reuse 20 existing IDs with different row content |

**Important:** NULL and invalid FK issues are **separate populations** — do not double-count the same row unless intentionally overlapping (document if overlap occurs).

---

## ID Ranges (Planned)

| Entity | Valid range | Invalid reference range |
|--------|-------------|-------------------------|
| customers | 1 – 10,000 | 90001+ for invalid FK targets |
| products | 1 – 500 | 9001+ for invalid FK targets |
| orders | 1 – 100,000 | Duplicates reuse IDs within range |

---

## Referential Integrity for Valid Rows

- Valid orders should reference `customer_id` ∈ [1, 10000] and `product_id` ∈ [1, 500]
- Generation order: **customers → products → orders** (masters before transactions)

---

## File Format

| Setting | Value |
|---------|-------|
| Format | CSV |
| Header | Yes |
| Encoding | UTF-8 |
| Delimiter | Comma |
| Quoting | Minimal (quote fields with commas if needed) |
| Date format | `YYYY-MM-DD` |

---

## Verification Checklist (Phase 1)

- [ ] `customers.csv` has exactly 10,000 rows
- [ ] `orders.csv` has exactly 100,000 rows
- [ ] `products.csv` has exactly 500 rows
- [ ] NULL email count = 50
- [ ] Duplicate customer_id count = 10 (rows involved)
- [ ] NULL customer_id on orders = 100
- [ ] NULL product_id on orders = 200
- [ ] Invalid customer_id on orders = 50
- [ ] Invalid product_id on orders = 30
- [ ] Duplicate order_id count = 20 (rows involved)
- [ ] All valid FKs resolve to master tables

---

## Status

| Task | Status |
|------|--------|
| Generator script | Not implemented |
| CSV files produced | Not done |
| Verification tests | Not implemented |

See `src/data_generation/DATA_GENERATION_NOTES.md` for module-level notes.
