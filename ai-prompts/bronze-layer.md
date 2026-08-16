# AI Prompt History — Bronze Layer

Log of AI prompts related to Bronze ingestion, schema handling, and ingestion metadata (Phase 2).

---

## Prompt 1 — Phase 2 Bronze layer implementation

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 2 — Bronze ingestion |
| **Files affected** | `src/bronze/*`, `tests/test_bronze_ingestion.py`, docs |

### Prompt purpose

Implement Bronze layer after Phase 1 completion: ingest CSVs to Delta with explicit schemas, metadata columns, ingestion log, error handling, configurable catalog/schema/paths, tests with local Spark fixtures, and documentation updates. Fix data-model/schema drift from Phase 0. No Silver/Gold work; no commit.

### AI response summary

- Aligned `data-model.md` and `database/schema.sql` with Phase 1 CSV contract
- Implemented `bronze_common.py` (schemas, config, read, metadata, Delta write, log)
- Created `01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`, `ingest_all.py`
- Write strategy: entity tables **overwrite**, ingestion log **append**
- Added `tests/test_bronze_ingestion.py` (local Spark, no Delta dependency in tests)
- Updated README, design-notes, setup-notes, debugging-notes

### Accepted decisions

- Explicit `StructType` schemas matching generator columns and nullability
- Metadata: `_ingestion_timestamp`, `_source_file`, `_batch_id`
- Env-based config (`BRONZE_CATALOG`, `BRONZE_SCHEMA`, `BRONZE_STORAGE_PATH`, `BRONZE_INPUT_DIR`)
- `prepare_bronze_dataframe()` as testable core without Delta write
- No business cleaning or Silver columns in Bronze

### Changed / rejected

| Item | Decision | Why |
|------|----------|-----|
| Phase 0 column names in data-model | **Updated** | Drift fix — match actual CSV |
| Delta write in local unit tests | **Rejected** | Avoid heavy delta-spark dep; document Databricks integration |
| Append mode for Bronze entities | **Rejected** | Overwrite chosen for deterministic dev |
| Streaming / Auto Loader | **Rejected** | Out of assessment scope |

### Why

Bronze must preserve raw data and DQ issues for Silver validation. Overwrite + ingestion log append balances simplicity with run auditability.

---

## Prompt 2 — Pre-commit Bronze engineering review

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 2 — Review / refinement |
| **Files affected** | `bronze_common.py`, tests, docs |

### Prompt purpose

Focused pre-commit review: raw fidelity, nullable FK STRING decision, schema/docs alignment, batch metadata, overwrite/append consistency, error handling, test gaps. Implement justified fixes only; no Silver work; no commit.

### AI response summary

Review confirmed Bronze does not clean/dedupe/filter. **STRING FK in Bronze** accepted as safest raw preservation for pandas `8952.0` CSV output; Phase 1 generator unchanged. Added tests: shared batch id/timestamp via `ingest_all_entities`, ingestion log row counts, invalid FK strings preserved, raw FK string format, header-only empty CSV. Narrowed CLI exception handling to `BronzeIngestionError`. Documented Bronze vs Silver FK normalization and idempotency trade-offs.

### Accepted decisions

- Keep nullable order FKs as STRING in Bronze; Silver parses `8952.0` → integer
- Do not change Phase 1 CSV generator (low risk improvement deferred)
- Entity overwrite + log append remains assessment default

### Changed / rejected

| Item | Decision | Why |
|------|----------|-----|
| Change generator to write integer CSV | **Rejected** | Bronze STRING approach already preserves fidelity; avoid Phase 1 churn |
| Broad `except Exception` in CLI | **Changed** to `BronzeIngestionError` | Clearer failure surface without hiding unexpected bugs in tests |

### Why

Raw fidelity in Bronze; type normalization belongs in Silver. Tests now cover coordinated batch metadata and invalid FK preservation.

---

## Future prompts

### Prompt N — (Template)

| Field | Detail |
|-------|--------|
| **Date** | |
| **Phase** | Bronze |
| **Files affected** | |

**Prompt summary:**  
**AI response summary:**  
**Accepted:**  
**Changed:**  
**Rejected:**  
**Why:**
