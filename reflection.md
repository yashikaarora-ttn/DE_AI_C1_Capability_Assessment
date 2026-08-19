# Reflection — Data Engineering AI Capability Assessment

Personal reflection on the Databricks Medallion pipeline project. Content below is grounded in **repository evidence** (code, tests, `debugging-notes.md`, `ai-prompts/`) and includes the candidate's personal reflection.

---

## Project Overview

| Field | Value |
|-------|-------|
| **Project** | Databricks Medallion Pipeline — E-Commerce Analytics |
| **Assessment** | AI Capability Assessment (C1) — Databricks Medallion Pipeline |
| **Track** | Track 1 — Up to TL Level |
| **Completion status** | Phases 0–5 implementation complete; Phases 6–7 documentation |
| **Regression evidence** | 127 tests passed (compliance audit) |

---

## What AI Helped With

AI assistance through Cursor (Company Account) was used across the lifecycle documented in `ai-prompts/` and `tool-workflow.md`. Cursor Auto supported general development and routine tasks, while Cursor Grok 4.6 with high reasoning effort supported complex coding, debugging, engineering reviews, and validation. The models were used as appropriate rather than exclusively. Drafts were reviewed against tests, reconciliation checks, and pre-commit reviews before acceptance — AI did not make final engineering decisions without validation.

| Area | Evidence |
|------|----------|
| **Architecture & requirements planning** | Medallion layer design, phased plan, repository structure (`ai-prompts/documentation.md` Prompts 1–3) |
| **Deterministic data generation** | Seed-42 generator with exact intentional DQ counts (`src/data_generation/`, 17 tests) |
| **Bronze ingestion design** | Explicit CSV schemas, ingestion metadata, overwrite + log append strategy |
| **Silver DQ implementation** | Completeness, type/business rules, uniqueness, RI, metrics (`src/silver/`, 52 Silver-related tests) |
| **Gold aggregations** | `trusted_business_orders` policy, reconciliation tests (`src/gold/`, Issue 009) |
| **Dashboard SQL** | Gold-only query assets and static contract tests (`src/dashboard/`, 25 tests) |
| **Test generation & refinement** | Layer tests grew from data-gen through full regression (127 total) |
| **Debugging** | Nine documented issues with fixes (`debugging-notes.md`, `ai-prompts/debugging.md`) |
| **Documentation & compliance review** | Cross-doc consistency, submission readiness (compliance audit) |

AI accelerated scaffolding and iteration; outputs were not accepted without review and test evidence.

---

## Where Engineering Judgment Was Required

These decisions required human review beyond generated code:

| Decision | Rationale (evidence) |
|----------|----------------------|
| **Duplicate-row semantics** | Duplicate `customer_id` / `order_id` groups flag **all** rows in the group, not just the second occurrence — documented in `DATA_GENERATION_NOTES.md` and uniqueness tests |
| **Bronze STRING FKs for orders** | Preserve pandas CSV values like `8952.0`; Silver normalizes to INTEGER (Issue 005) |
| **NULL vs malformed vs invalid-reference FK** | Distinct reason codes: `NULL_*`, `INVALID_*_TYPE`, `INVALID_*` (RI) — not conflated |
| **Retain failed Silver rows** | `dq_status` / `dq_failure_reasons`; no silent deletion — Gold filters trusted rows only |
| **`trusted_business_orders` for Gold revenue** | Product and customer revenue reconciled only when PASS customer **and** PASS product join (Issue 009) |
| **Local vs Databricks claims** | Delta writes and dashboard UI documented as prepared but **not executed** in this repo |
| **Dashboard revenue bands** | Presentation assumptions (500/2000/5000) separate from Gold High-Value threshold (1000) |
| **Thin `05_quality_business_logic.py`** | Delegates to type module to avoid misleading duplication — assignment alignment |

The decisions I considered most carefully were how to preserve raw data in Bronze while still enabling reliable validation in Silver, and how to define a consistent trusted-data boundary for Gold. I also had to ensure that data-quality issues remained visible rather than being silently removed, so that the pipeline stayed traceable and explainable.

---

## What Changed Because of Testing and Review

Real changes driven by tests and pre-commit reviews (`debugging-notes.md` Issues 001–009):

| Issue | Change |
|-------|--------|
| 001 | Fixed FK count operator precedence in data generator |
| 002 | Tightened FK validity test exclusions |
| 003 | Capped payment dates at today |
| 004 | Fixed Faker reproducibility (`seed_instance`) |
| 005 | Bronze order FKs as STRING |
| 006 | Local Spark `file://` paths in tests |
| 007 | Explicit schemas for NULL fixture rows |
| 008 | Single shared `spark` fixture in `conftest.py` |
| 009 | Introduced `trusted_business_orders()` for Gold reconciliation |

Pre-commit reviews (documented in layer `ai-prompts/`) caught documentation drift and SQL/dashboard contract gaps before commit.

---

## Limitations

| Limitation | Detail |
|------------|--------|
| **Databricks Delta writes** | Code paths exist (`create_*_tables.py`); not integration-tested in this local repository |
| **Databricks SQL Warehouse / dashboard UI** | SQL assets and setup guide prepared; rendering not executed here |
| **Synthetic data** | Deterministic seed-42 CSVs with intentional DQ issues — not production data |
| **Threshold assumptions** | `GOLD_HIGH_VALUE_THRESHOLD` (default 1000) and dashboard revenue bands are documented assessment assumptions |
| **Curated AI prompt history** | `ai-prompts/` summarizes decisions/outcomes; not a verbatim Cursor transcript archive |
| **Test scope** | 127 pytest tests validate logic and contracts locally — not a substitute for cluster integration |

---

## Key Learning

The project demonstrates that AI-assisted development is most effective when paired with **explicit validation evidence**: deterministic generators with exact DQ counts, layer tests, seed-42 integration runs, Gold revenue reconciliation, and honest documentation of what was not run on Databricks.

Useful patterns from this assessment:

- Flag-not-delete DQ with stable reason codes before aggregation
- Single trusted business-order definition for reconcilable Gold metrics
- Static SQL contract tests for dashboard assets without claiming Databricks execution
- Phase-wise prompt documentation plus debugging issue log for auditability

My key learning was that AI can significantly speed up implementation, debugging, and review, but the output still needs to be validated with engineering judgment. I found the best results came from giving clear constraints, reviewing the generated approach, testing it against expected data behaviour, and refining it when the results exposed an issue.

---

## Self-Assessment

| Criterion | Rating (1–5) | Notes |
|-----------|--------------|-------|
| Prompt Engineering | 3.5/5 | Scoped prompts by phase and refined outputs against evidence |
| Cursor / AI Collaboration | 3.5/5 | Used AI for development, review, and documentation with human validation |
| PySpark / Databricks | 4/5 | Implemented the local Medallion pipeline; Databricks runtime was not validated |
| Testing / Debugging | 3.5/5 | 127 local regression tests and nine documented issues |

---

## Final Thoughts

This assessment gave me practical experience in using AI as part of an end-to-end data engineering workflow rather than only as a code-generation tool. The final solution has been reviewed through incremental development, automated tests, data-quality validation, and reconciliation, and I am comfortable explaining the design decisions and trade-offs made throughout the implementation.
