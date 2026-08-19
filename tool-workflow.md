# AI Tool Workflow

Structured record of how **Cursor** (AI-assisted IDE) was used throughout this assessment.

---

## End-to-End Workflow

Each phase followed this cycle:

```text
Requirement understanding
    → prompt (scoped, with constraints)
    → AI implementation draft
    → pytest / inspection
    → refine or debug
    → pre-commit review (where applicable)
    → regression
    → commit (after review checkpoint)
```

**Commits were not automatic.** Changes were committed after review and test evidence — not every AI suggestion was accepted.

### Example: Gold reconciliation (Issue 009)

1. **Requirement:** Product and customer Gold revenue must reconcile.
2. **Test failure:** `test_gold_aggregations.py` showed ~265k gap on seed-42 data.
3. **Root cause:** PASS Completed orders to FAIL customers counted in product sales only.
4. **Fix:** `trusted_business_orders()` as single realized-revenue basis.
5. **Regression:** 127 tests passed; documented in `gold_common.py` and design docs.
6. **Commit:** After review (commit `398890c` and related).

### Example: Bronze FK representation (Issue 005)

1. **Symptom:** 100k null `customer_id` after Bronze read.
2. **AI-assisted diagnosis:** pandas `8952.0` vs Spark INTEGER parsing.
3. **Decision:** STRING FK in Bronze; Silver normalizes.
4. **Validation:** Bronze ingestion tests; prevention in schema docs.

---

## Primary AI Tool

| Field | Value |
|-------|-------|
| **Tool name** | Cursor (Company Account) |
| **Models used** | Cursor Auto; Cursor Grok 4.6 (High reasoning effort) |
| **Usage period** | 17 Aug 2026 – 20 Aug 2026 |

Cursor Auto supported general development and routine tasks. Cursor Grok 4.6 with high reasoning effort supported complex coding, debugging, engineering reviews, and validation. Model selection was task-dependent; neither model was used exclusively.

---

## Project Context

Databricks Medallion pipeline (Bronze → Silver → Gold → Dashboard) for e-commerce CSV analytics. Cursor used for requirements planning, phased implementation, test-driven validation, pre-commit reviews, and compliance audit. Human review before accepting generated code and documentation.

---

## Requirement Analysis

**Status:** Initial analysis completed (see `requirements-analysis.md` and `ai-prompts/documentation.md`).

| Activity | AI involvement | Outcome |
|----------|----------------|---------|
| Initial requirements review | AI partner analyzed assessment requirements and produced planning document | Accepted as basis for `requirements-analysis.md` |
| Clarifications / decisions | Repo flatten; `dq_status` model; Gold trusted-business-order policy | Documented in `ai-prompts/` |

---

## Pipeline Design

**Status:** Initial design documented (see `design-notes.md`, `data-model.md`, `data-quality-strategy.md`).

| Activity | AI involvement | Outcome |
|----------|----------------|---------|
| Medallion architecture design | Phase 0 planning + `design-notes.md` | Bronze → Silver → Gold → Dashboard |
| Data model definition | `data-model.md`, `database/schema.sql` | Explicit schemas per layer |
| DQ strategy | `data-quality-strategy.md` | Four dimensions; flag-not-delete |

---

## Code Generation

| Phase | Files / modules | AI role | Human review notes |
|-------|-----------------|---------|-------------------|
| Phase 0 — Foundation | Documentation, structure | Planning, templates, repo flatten | Reviewed before Phase 1 |
| Phase 1 — Data generation | `generate_sample_data.py`, tests | Implementation + DQ count validation | Pre-commit review; Issues 001–004 |
| Phase 2 — Bronze | `src/bronze/*`, tests | Schemas, ingest, Delta write pattern | Pre-commit review; Issues 005–006 |
| Phase 3 — Silver | `src/silver/*`, tests | DQ rules, metrics, orchestration | Full pipeline tests; Issues 007–008 |
| Phase 4 — Gold | `src/gold/*`, tests | Aggregations, reconciliation | Issue 009; pre-commit review |
| Phase 5 — Dashboard | `src/dashboard/*`, static tests | SQL assets, setup guide | Pre-commit review |
| Tests / regression | `tests/*`, `conftest.py` | Fixture design, contracts | 127 tests at audit |
| Phase 6 — Submission docs | Candidate details, reflection, checklist | Documentation polish | Complete |

---

## Validation

AI-assisted code validated via `pytest` (127 tests at compliance audit), static SQL contract tests for dashboard, seed-42 generated-data integration tests for Silver/Gold, and manual doc review. Databricks Delta writes and SQL Warehouse dashboard execution **not validated in this repository** — documented honestly in README and setup notes.

---

## Testing

| Test type | AI contribution | Result |
|-----------|-----------------|--------|
| Unit / layer tests | Generated tests alongside validators | 127 passed (audit) |
| Integration (Silver/Gold) | Seed-42 pipeline fixtures | Pass |
| DQ validation tests | Exact intentional-issue counts | Pass |
| Dashboard SQL contracts | Static Gold-only checks | 25 passed |
| Databricks cluster execution | Not run in repo | Pending evaluator environment |

---

## Debugging

Nine real issues logged in `debugging-notes.md`; AI-assisted diagnosis summaries in `ai-prompts/debugging.md` (Issues 001–009). No fabricated conversation transcripts.

---

## Data Quality

AI helped design the four Silver DQ dimensions (completeness, type/business, uniqueness, RI), stable reason codes, `silver_dq_metrics` with pass/fail percentages, and Gold trusted-data policy. See `data-quality-strategy.md` and `ai-prompts/silver-layer.md`.

---

## Responsible AI

| Principle | How it was applied |
|-----------|-------------------|
| **Transparency** | All significant AI interactions logged in `ai-prompts/` |
| **Verification** | Code and docs reviewed before acceptance; no unverified execution claims |
| **No fabricated results** | Test outcomes and run logs documented only after actual execution |
| **Security** | No secrets pasted into AI tools; placeholders used for credentials |
| **Scope discipline** | AI suggestions evaluated against 20–25 hour assessment scope |

Additional detail in `final-ai-usage-summary.md` and `reflection.md`.

---

## Lessons Learned

- Scoped prompts per phase reduced rework (e.g. Silver foundation before uniqueness/RI).
- Test-first validation caught reconciliation gaps early (Issue 009).
- Curated `ai-prompts/` plus `debugging-notes.md` provides audit trail without claiming false Databricks execution.
- Candidate metadata and personal reflection statements are complete; optional raw Cursor transcript export and final manual submission actions remain.

One of my main lessons was that breaking a larger problem into smaller, reviewable phases makes AI-assisted development much more effective. Clear prompts, incremental commits, automated testing, and validation of actual outputs helped me identify issues early and improve the solution without losing control of the overall design.

---

## Workflow Summary

| Stage | Completed | Notes |
|-------|-----------|-------|
| Requirements analysis | Yes | `ai-prompts/documentation.md` Prompt 1 |
| Foundation / docs | Yes | Phase 0 + repo flatten |
| Data generation | Yes | Seed 42; 17 tests |
| Bronze layer | Yes | 17 tests |
| Silver layer | Yes | 52 tests |
| Gold layer | Yes | 16 tests |
| Dashboard | Yes | 25 SQL contract tests |
| Testing | Yes | 127 total regression |
| Final reflection / submission docs | **Yes** | Metadata and candidate narrative complete |
