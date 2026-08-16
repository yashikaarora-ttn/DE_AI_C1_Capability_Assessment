# Debugging Notes

Structured log of issues encountered and resolutions during pipeline development. **No issues logged yet** — template for use during implementation.

---

## How to Use This Document

For each issue, add an entry with:

1. **Symptom** — What failed or looked wrong
2. **Context** — Layer, file, command, environment
3. **Root cause** — Why it happened
4. **Resolution** — Fix applied
5. **Prevention** — Test or doc update to avoid recurrence

Also capture related AI prompts in `ai-prompts/debugging.md`.

---

## Environment Reference

| Field | Value |
|-------|-------|
| Databricks runtime | `[NOT CONFIGURED]` |
| Cluster type | `[NOT CONFIGURED]` |
| Catalog / schema | `[NOT CONFIGURED]` |
| Local Python version | 3.13.3 (venv) |

---

## Issue Log

### Issue 001 — `count_invalid_foreign_keys` returned negative count

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 1 — Data generation |
| **Symptom** | `generate_sample_data.py` failed with `Invalid customer_id count does not match target` |
| **Context** | `python src/data_generation/generate_sample_data.py` during Phase 1 validation |
| **Root cause** | Operator precedence bug: `~non_null.isin(parent_keys).sum()` applied bitwise `~` to the integer sum instead of negating the boolean Series |
| **Resolution** | Changed to `(~non_null.isin(parent_keys)).sum()` with explicit parentheses |
| **Prevention** | FK validation tests; clearer boolean expression in helper |

### Issue 002 — FK validity test did not exclude invalid FK rows

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 1 — Data generation |
| **Symptom** | `test_valid_foreign_keys_after_excluding_intentional_issues` failed: 50 invalid customer refs remained |
| **Context** | `pytest tests/test_data_generation.py` |
| **Root cause** | Test filtered NULLs and duplicate order_ids but not non-null invalid FK values |
| **Resolution** | Extended test filter to require FK values exist in parent tables; assert `len(valid_orders) == CLEAN_ORDER_COUNT` |
| **Prevention** | Test now matches documented exclusion criteria |

---

### Issue 003 — Completed orders could have future payment_date

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 1 — Pre-commit review |
| **Symptom** | Potential future `payment_date` when `order_date` is recent and status is Completed |
| **Context** | Code review of `_payment_date_for_status` in `generate_sample_data.py` |
| **Root cause** | Payment offset added to `order_date` without capping at today's date |
| **Resolution** | `return min(payment, today)` for Completed; Cancelled payment dates also capped |
| **Prevention** | `test_dates_are_not_in_the_future`; validation in `_validate_order_frame` |

---

### Issue 004 — `Faker(seed=)` broke reproducibility in pytest

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 1 — Pre-commit review |
| **Symptom** | `test_same_seed_produces_reproducible_output` failed after switching to `Faker(seed=seed)` |
| **Context** | `pytest tests/test_data_generation.py` during review refinements |
| **Root cause** | `Faker(seed=...)` did not yield identical sequences across repeated `generate_all` calls in the same process |
| **Resolution** | Use `faker = Faker(); faker.seed_instance(seed)` in generator and test fixtures |
| **Prevention** | Reproducibility integration test retained |

---

### Issue 005 — Spark CSV INT nulls for pandas float-formatted FK values

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 2 — Bronze |
| **Symptom** | Bronze tests showed all `customer_id` null after read (100k nulls) |
| **Context** | `pytest tests/test_bronze_ingestion.py` |
| **Root cause** | Pandas writes nullable integer columns as floats in CSV (`8952.0`); Spark `IntegerType` cannot parse and sets null |
| **Resolution** | Bronze reads nullable order FK columns as `STRING`; documented for Silver casting |
| **Prevention** | Bronze schema docs; intentional null count tests |

### Issue 006 — Local Spark HDFS connection errors on CSV read

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 2 — Bronze |
| **Symptom** | `Connection refused` to corporate HDFS host when reading local CSV |
| **Context** | Local PySpark tests on macOS |
| **Root cause** | Default Hadoop FS not `file://`; plain paths resolved to HDFS |
| **Resolution** | Use `file://` URIs via `Path.as_uri()` and `spark.hadoop.fs.defaultFS=file:///` |
| **Prevention** | Spark test fixture config |

---

### Issue N — (Template)

| Field | Detail |
|-------|--------|
| **Date** | `[DATE]` |
| **Phase** | `[e.g., Silver validation]` |
| **Symptom** | `[DESCRIPTION]` |
| **Context** | `[FILE, COMMAND, NOTEBOOK]` |
| **Root cause** | `[CAUSE]` |
| **Resolution** | `[FIX]` |
| **Prevention** | `[TEST / DOC UPDATE]` |

---

## Common Problem Areas (Anticipated)

| Area | What to watch for |
|------|-------------------|
| Bronze ingestion | Schema mismatch; path errors; CSV parsing |
| Silver validation | Rule logic; duplicate detection; FK join performance |
| Gold aggregation | Revenue double-count from duplicate orders |
| DQ metrics | Percentage calculation; per-rule vs per-row counting |
| Databricks setup | Catalog permissions; storage credentials |
| Tests | Local Spark vs cluster differences |

---

## Useful Commands (Planned)

```bash
# Run unit tests (when implemented)
pytest tests/ -v

# Check generated CSV row counts (Phase 1)
# wc -l data/raw/*.csv
```

```sql
-- Check Silver invalid row sample (when implemented)
-- SELECT * FROM <schema>.silver_orders WHERE is_valid = false LIMIT 20;
```

---

## Status

| Metric | Value |
|--------|-------|
| Issues logged | 6 |
| Open issues | 0 |
| Resolved issues | 6 |
