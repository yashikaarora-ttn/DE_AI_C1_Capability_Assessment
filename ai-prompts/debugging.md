# AI Prompt History — Debugging

Log of AI-assisted debugging during pipeline development. Entries summarize **real issues** from `debugging-notes.md` — not fabricated conversation transcripts.

Cross-reference: `debugging-notes.md` (Issues 001–009).

---

## Issue 001 — Data generation FK count (operator precedence)

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 001 |
| **Phase** | Data generation |
| **Files affected** | `src/data_generation/generate_sample_data.py`, `tests/test_data_generation.py` |

**Symptom:** Generator validation failed — invalid `customer_id` count mismatch.

**Root cause:** Bitwise `~` applied to integer sum instead of boolean Series in FK count helper.

**AI-assisted resolution:** Identified precedence bug; fix with `(~non_null.isin(parent_keys)).sum()`.

**Validation:** Generator runs; FK count tests pass.

---

## Issue 002 — FK validity test scope

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 002 |
| **Phase** | Data generation / tests |
| **Files affected** | `tests/test_data_generation.py` |

**Symptom:** `test_valid_foreign_keys_after_excluding_intentional_issues` failed.

**Root cause:** Test excluded NULLs and duplicate order ids but not invalid non-null FK values.

**AI-assisted resolution:** Extended exclusion filter to match documented intentional-issue categories.

**Validation:** `pytest tests/test_data_generation.py` passes.

---

## Issue 003 — Future payment dates

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 003 |
| **Phase** | Data generation (pre-commit review) |
| **Files affected** | `src/data_generation/generate_sample_data.py` |

**Symptom:** Completed orders could have `payment_date` after today.

**Root cause:** Payment offset from `order_date` without cap at current date.

**AI-assisted resolution:** `min(payment, today)` in payment-date helper.

**Validation:** `test_dates_are_not_in_the_future`.

---

## Issue 004 — Faker reproducibility

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 004 |
| **Phase** | Data generation (pre-commit review) |
| **Files affected** | `src/data_generation/generate_sample_data.py`, tests |

**Symptom:** Same-seed reproducibility test failed after `Faker(seed=seed)`.

**Root cause:** `Faker(seed=...)` did not reproduce across repeated calls in one process.

**AI-assisted resolution:** `faker.seed_instance(seed)` pattern.

**Validation:** `test_same_seed_produces_reproducible_output`.

---

## Issue 005 — Bronze STRING FK for pandas CSV floats

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 005 |
| **Phase** | Bronze |
| **Files affected** | `src/bronze/bronze_common.py`, Bronze schemas |

**Symptom:** All order `customer_id` null after Bronze read (100k nulls).

**Root cause:** Pandas writes `8952.0`; Spark `IntegerType` nulls unparseable values.

**AI-assisted resolution:** Store nullable order FKs as STRING in Bronze; Silver normalizes.

**Validation:** Bronze ingestion tests; documented in `bronze_common.py`.

---

## Issue 006 — Local Spark HDFS path resolution

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 006 |
| **Phase** | Bronze / local tests |
| **Files affected** | `src/bronze/bronze_common.py`, `tests/conftest.py` |

**Symptom:** Connection refused to HDFS when reading local CSV.

**Root cause:** Default FS not `file://` on macOS.

**AI-assisted resolution:** `file://` URIs and `spark.hadoop.fs.defaultFS=file:///` in test fixture.

**Validation:** Local Bronze/PySpark tests run without cluster.

---

## Issue 007 — Spark NULL type inference (3.13)

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 007 |
| **Phase** | Silver tests |
| **Files affected** | `tests/test_silver_*.py`, `tests/silver_test_fixtures.py` |

**Symptom:** `CANNOT_DETERMINE_TYPE` for single-row fixtures with NULL.

**Root cause:** `createDataFrame` without explicit `StructType` for nullable columns.

**AI-assisted resolution:** Reuse Bronze CSV schemas in Silver tests; shared fixtures.

**Validation:** Silver unit tests pass.

---

## Issue 008 — Duplicate Spark session fixtures

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Issue reference** | `debugging-notes.md` Issue 008 |
| **Phase** | Silver tests |
| **Files affected** | `tests/conftest.py`, test modules |

**Symptom:** Combined `pytest tests/` failed with multiple session `spark` fixtures.

**Root cause:** Bronze and Silver modules each defined session-scoped `spark`.

**AI-assisted resolution:** Single `spark` fixture in `tests/conftest.py`.

**Validation:** Full regression suite runs in one session.

---

## Issue 009 — Gold revenue reconciliation mismatch

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-17 |
| **Issue reference** | `debugging-notes.md` Issue 009 |
| **Phase** | Gold |
| **Files affected** | `src/gold/gold_common.py`, `tests/test_gold_aggregations.py`, Gold SQL |

**Symptom:** Product-level vs customer-level `total_revenue` sums diverged on seed-42 data.

**Root cause:** PASS Completed orders to FAIL customers counted in product sales but not customer revenue.

**AI-assisted resolution:** `trusted_business_orders()` as single realized-revenue basis for all Gold metrics.

**Validation:** Reconciliation tests pass; documented in `gold_common.py` and design docs.

---

## Status

Nine documented issues with symptom → root cause → fix → test validation. No fabricated prompt transcripts; see layer-specific `ai-prompts/` for implementation prompts.
