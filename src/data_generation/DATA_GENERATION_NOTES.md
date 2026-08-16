# Data Generation Notes

Module: `src/data_generation/generate_sample_data.py` — **Implemented** (Phase 1).

---

## Purpose

Generate three realistic CSV datasets for the e-commerce Medallion pipeline:

| File | Rows |
|------|------|
| `customers.csv` | 10,000 |
| `orders.csv` | 100,000 |
| `products.csv` | 500 |

Datasets include **intentional data-quality issues** at exact counts required by the assessment.

---

## Duplicate Row Definition

Used consistently by the generator and `tests/test_data_generation.py`:

> For a primary-key column, the **duplicate row count** is the number of rows whose key value appears **more than once** in the dataset. Every row belonging to a duplicated key is counted.

| Assessment target | Implementation |
|-------------------|----------------|
| 10 duplicate `customer_id` rows | 5 customer_ids (1–5) each appear **exactly twice** → 10 rows |
| 20 duplicate `order_id` rows | 10 order_ids each appear **exactly twice** → 20 rows |

---

## Intentional DQ Issues (Disjoint Categories)

### Customers

| Issue | Count | Notes |
|-------|-------|-------|
| NULL `email` | 50 | Applied to rows with `customer_id` > 5 (disjoint from duplicate-id rows) |
| Duplicate `customer_id` rows | 10 | ids 1–5 each appear twice |

### Orders

Order issue categories are **disjoint row sets** (no overlap):

| Issue | Count |
|-------|-------|
| NULL `customer_id` | 100 |
| NULL `product_id` | 200 |
| Invalid `customer_id` (non-null, not in customers) | 50 (IDs 90001–90050) |
| Invalid `product_id` (non-null, not in products) | 30 (IDs 9001–9030) |
| Duplicate `order_id` rows | 20 (10 ids × 2 rows) |

Clean valid orders: **99,600** rows with unique `order_id` and valid FKs.

---

## Module Structure

```text
src/data_generation/
├── __init__.py
├── generate_sample_data.py   # Generator, DQ counters, CLI
└── DATA_GENERATION_NOTES.md
```

Key functions:

- `generate_customers()`, `generate_products()`, `generate_orders()`
- `generate_all(seed, output_dir)` — full pipeline
- `count_duplicate_key_rows()`, `count_null_emails()`, `count_invalid_foreign_keys()` — shared with tests

---

## Output Location

| Path | Description |
|------|-------------|
| `data/customers.csv` | Customer master |
| `data/products.csv` | Product catalog |
| `data/orders.csv` | Order transactions |

Files are **gitignored** (see root `.gitignore`).

---

## Usage

From repository root (use a virtual environment on externally managed Python installs):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
pytest tests/test_data_generation.py -v
```

---

## Reproducibility

- Default seed: `42` (`DEFAULT_SEED` in module)
- Uses `random.Random(seed)` and `faker.seed_instance(seed)` on a dedicated Faker instance
- Same seed → identical CSV output

---

## Dependencies

See `requirements.txt`: `pandas`, `faker`, `pytest`.

---

## Related Documentation

- `database/seed-data-notes.md` — setup and verification checklist
- `data-model.md` — entity schemas (note: Phase 1 CSV columns match generator output)
- `data-quality-strategy.md` — Silver rules that must detect these issues
- `tests/test_data_generation.py` — automated verification
- `ai-prompts/data-generation.md` — AI prompt history

---

## Status

| Task | Status |
|------|--------|
| Generator implementation | Done |
| DQ injection logic | Done |
| Verification tests | Done |
| CSV output | Run generator to produce |
