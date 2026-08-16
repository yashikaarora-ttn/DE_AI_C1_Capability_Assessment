# Data Generation Notes

Module: `src/data_generation/` — **Not yet implemented** (Phase 1).

---

## Purpose

Generate three realistic CSV datasets for the e-commerce Medallion pipeline:

- `customers.csv` — 10,000 rows
- `orders.csv` — 100,000 rows
- `products.csv` — 500 rows

Datasets must include **intentional data-quality issues** at exact counts specified in `data-quality-strategy.md`.

---

## Planned Module Structure

```text
src/data_generation/
├── generate_datasets.py    # Main entry point (planned)
├── config.py               # Row counts, seed, output path (planned)
└── inject_dq_issues.py     # DQ defect injection logic (planned)
```

---

## Dependencies (Planned)

- `pandas` or pure Python CSV writer
- `faker` (optional) for realistic names/emails
- `pytest` for count verification tests

Dependencies will be added to `requirements.txt` in Phase 1.

---

## Output

| Path | Description |
|------|-------------|
| `data/raw/customers.csv` | Customer master |
| `data/raw/orders.csv` | Order transactions |
| `data/raw/products.csv` | Product catalog |

Generated files will be gitignored (see root `.gitignore`).

---

## Related Documentation

- `database/seed-data-notes.md` — Issue counts and ID ranges
- `data-model.md` — Column definitions
- `data-quality-strategy.md` — Validation rules that must detect injected issues
- `ai-prompts/data-generation.md` — AI prompt history for this phase

---

## Status

| Task | Status |
|------|--------|
| Generator implementation | Not started |
| DQ injection logic | Not started |
| Verification tests | Not started |
| CSV output | Not generated |
