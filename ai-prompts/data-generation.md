# AI Prompt History — Data Generation

Log of AI prompts related to CSV dataset generation and DQ issue injection (Phase 1).

---

## Prompt 1 — Phase 1 sample data generation

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 1 — Sample Data Generation |
| **Files affected** | `generate_sample_data.py`, `tests/test_data_generation.py`, `requirements.txt`, docs |

### Prompt purpose

Implement a deterministic, testable Python sample-data generator after Phase 0 was committed. Required exact row counts, specific CSV schemas (customer_name, unit_price, total_amount, etc.), intentional DQ issues at exact counts, clear duplicate row definitions, disjoint order issue categories, automated pytest coverage, and documentation updates. Explicitly excluded Bronze/Silver/Gold/Dashboard work and git commit.

### AI response summary

- Implemented `src/data_generation/generate_sample_data.py` with modular generators, shared DQ counting helpers, CLI, and repo-relative paths
- Documented duplicate row definition in module docstring and tests
- Generated customers (9995 unique + 5 duplicate pairs), products (500), orders (99600 clean + disjoint issue sets)
- Created `tests/test_data_generation.py` with 13 tests covering all required validations
- Added minimal `requirements.txt` (pandas, faker, pytest)
- Updated README, `DATA_GENERATION_NOTES.md`, `seed-data-notes.md`, and this file

### Accepted design decisions

- **Duplicate definition:** Count all rows whose PK appears more than once (not just "extra" copies)
- **Customer duplicates:** 5 ids (1–5) × 2 rows = 10 duplicate rows
- **Order duplicates:** 10 ids × 2 rows = 20 duplicate rows
- **Disjoint order DQ sets:** NULL, invalid FK, and duplicate categories on separate rows
- **Invalid FK ranges:** customer 90001–90050, product 9001–9030
- **Output directory:** `data/` at repo root (gitignored CSVs)
- **Default seed:** 42 for reproducibility
- **Dependencies:** pandas + Faker + pytest only

### Changed / rejected

| Item | Decision | Why |
|------|----------|-----|
| `data/raw/` output path | **Changed** to `data/` | User specified `data/` in Phase 1 prompt |
| Phase 0 `data-model.md` column names | **Not updated yet** | User scoped Phase 1 to generator only; data-model still reflects earlier draft |
| Overlap of DQ categories | **Rejected** | User required independent issue counts for validation |
| Heavy dependencies | **Rejected** | stdlib + pandas/Faker only as specified |

### Why

Phase 1 delivers reproducible sample data with verifiable DQ defects required by Silver layer testing later. Clear duplicate definitions prevent ambiguous test failures. Disjoint issue rows ensure each DQ metric can be validated independently.

---

## Prompt 2 — Pre-commit engineering review and refinement

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 1 — Review / refinement |
| **Files affected** | `generate_sample_data.py`, `tests/test_data_generation.py`, docs |

### Prompt purpose

Focused engineering review before commit: correctness (disjoint DQ, duplicates, FKs, dates), maintainability, reproducibility, test quality, dependencies, and documentation consistency. Implement only justified fixes without changing dataset sizes or DQ counts; preserve passing behavior; no Bronze/Silver/Gold work.

### AI response summary

Review identified no must-fix correctness bugs in DQ counts. Applied targeted improvements:

- Capped `payment_date` at today for Completed/Cancelled orders (avoid future payment dates)
- Validated non-null email format and past/today dates in generator self-checks
- Switched to `faker.seed_instance(seed)` for reproducible Faker output (constructor seed alone was insufficient in pytest)
- Removed unused `nonlocal order_id` in `append_order`
- Added tests: disjoint order DQ categories, customer null-email vs duplicate disjointness, email format, dates not in future
- Fixed `seed-data-notes.md` order_id range documentation (100k rows, max id 99,990)
- Aligned `DATA_GENERATION_NOTES.md` usage with venv/README commands

### Accepted design decisions

- Keep list-of-dicts + single DataFrame conversion (adequate performance for 100k rows)
- Keep shared DQ counting helpers imported by tests (practical for assessment scope)
- No dependency changes — pandas, faker, pytest all still required

### Changed / rejected

| Item | Decision | Why |
|------|----------|-----|
| Split `generate_orders` into smaller modules | **Rejected** | Assessment clarity; function is long but linear |
| Optimize `iterrows` test loop | **Rejected** | Acceptable for test suite size; clarity over micro-optimization |
| Update `data-model.md` | **Deferred** | Out of review scope; noted for later alignment |

### Why

Pre-commit review caught documentation drift and strengthened tests around disjoint DQ categories and date realism without altering assessment DQ targets.

---

## Future prompts

### Prompt N — (Template)

| Field | Detail |
|-------|--------|
| **Date** | |
| **Phase** | Data generation |
| **Files affected** | |

**Prompt summary:**  
**AI response summary:**  
**Accepted:**  
**Changed:**  
**Rejected:**  
**Why:**
