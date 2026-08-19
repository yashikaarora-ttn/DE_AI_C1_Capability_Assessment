# AI Prompt History — Documentation & Planning

Log of AI prompts related to requirements analysis, planning, documentation, and repository foundation.

---

## Prompt 1 — Requirements analysis and implementation planning

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Pre-implementation / planning |
| **Activity area** | Requirements analysis, architecture planning |

### Prompt text (summary)

Requested AI partnership for a Data Engineering AI Capability Assessment. Asked for analysis of a Databricks Medallion Architecture pipeline (Bronze → Silver → Gold → Dashboard) for e-commerce CSV data with intentional DQ issues, tests, and documentation. Explicitly requested **no implementation code** — only structured planning covering business problem, functional/non-functional requirements, assumptions, edge cases, technology choices, architecture, repo structure, testing strategy, DQ strategy, phased plan, and risks.

### AI response summary

AI produced a 12-section planning document including:

- Concise business problem statement
- Functional and non-functional requirements tables
- Assumptions (Databricks, Delta, revenue formula, segmentation approach)
- Edge cases (duplicates, NULL vs invalid FK, Gold deduplication)
- Technology recommendations (PySpark, Delta, pytest, Databricks SQL Dashboard)
- Bronze/Silver/Gold architecture with mermaid diagram
- Recommended repository structure under modular `src/` layout
- Testing pyramid (unit + small Spark integration)
- DQ validation strategy with rule matrix and metrics table
- 7-phase implementation plan (~20–25 hours)
- Risks and ambiguities table with recommendations

### What was accepted

- Medallion layer responsibilities and data flow (Bronze raw → Silver validated → Gold aggregated → Dashboard)
- Flag-not-delete DQ approach with `is_valid` and reason codes
- Separate reason codes for NULL vs invalid FK references
- Phased implementation plan (Phase 0–7)
- Testing focus on generators, validators, and Gold logic with small fixtures
- Scope boundaries (no Airflow, streaming, or over-engineering)
- Revenue calculation as `quantity × product.price`
- Gold consumes only valid Silver rows

### What was changed

- Repository structure adapted to **assignment-required layout** (`databricks-medallion-pipeline/` with specific doc filenames: `candidate-info.md`, `tool-workflow.md`, `ai-prompts/`, etc.) rather than the AI-suggested `docs/` and `config/` layout
- Documentation split into assignment-mandated files (`requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`) instead of a single planning response
- `candidate-info.md` uses placeholders only — no invented personal data
- AI prompt logging structured per activity file under `ai-prompts/`
- `database/schema.sql` kept as placeholder DDL with environment variables — not full Medallion table creation yet

### What was rejected

- Immediate implementation of data generation, Bronze, Silver, Gold, or Dashboard code
- Adding `requirements.txt` and dependencies in Phase 0
- Creating reference DDL for all Bronze/Silver/Gold tables prematurely (left as commented placeholders)
- Enterprise additions: orchestration, CDC, multi-env CI/CD
- Claiming any Databricks execution or test results before they occur

### Why

Candidate requested plan review before coding. Assignment specifies a particular repository structure and phased delivery. Foundation phase should establish documentation and structure without premature code or fabricated execution outcomes. Scope discipline keeps the project within the 20–25 hour assessment boundary.

---

## Prompt 2 — Phase 0 foundation implementation

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 0 — Foundation |
| **Activity area** | Repository structure, documentation templates |

### Prompt text (summary)

After plan review, requested implementation of **Phase 0 only**: create assignment-required directory structure, documentation files, `.gitignore`, database placeholders, AI prompt templates, and structured templates for debugging/reflection — without data generation, pipeline code, CSV output, dependencies, or git commit.

### AI response summary

Created Phase 0 foundation under `databricks-medallion-pipeline/` subdirectory: documentation files, `database/` placeholders, `src/` layer directories, `ai-prompts/`, `.gitignore`, and empty `data/` and `tests/` folders. Root `README.md` pointed to the nested subdirectory.

### What was accepted

- Full documentation set (`requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`, etc.)
- Assignment-required file names and `ai-prompts/` structure
- `database/schema.sql` with environment placeholders
- `.gitignore` for Python, Databricks, IDE, and generated data
- Placeholder templates for `candidate-info.md`, `reflection.md`, debugging notes
- No pipeline code, CSVs, or dependencies in Phase 0

### What was changed

- Project placed in `databricks-medallion-pipeline/` subdirectory rather than repo root (corrected in Prompt 3)
- Repository structure adapted to assignment layout rather than AI-suggested `docs/` and `config/` layout

### What was rejected

- Data generation, Bronze/Silver/Gold/Dashboard implementation
- `requirements.txt` in Phase 0
- Git commit (candidate review first)
- Fabricated test or execution results

### Why

Candidate requested plan review before coding. Phase 0 scope limited to structure and documentation only.

---

## Prompt 3 — Flatten repository structure to Git root

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-16 |
| **Phase** | Phase 0 — Foundation (structural correction) |
| **Activity area** | Repository layout refactor |

### Prompt text (summary)

After Phase 0 review, candidate identified a structural issue: assessment project lived in `databricks-medallion-pipeline/` while the Git repository root is `DE_AI_C1_Capability_Assessment/`. Requested moving all project contents to the repository root, merging READMEs, preserving documentation and `.gitignore`, removing the empty nested directory, and **not** starting Phase 1 or committing.

### AI response summary

Moved all files and directories from `databricks-medallion-pipeline/` to the repository root (`src/`, `data/`, `database/`, `tests/`, `ai-prompts/`, and all markdown documentation). Merged nested and root README content into a single root `README.md`. Updated `requirements-analysis.md` repository layout decision. Removed `databricks-medallion-pipeline/` directory. Did not modify `.git/` or create a nested repository.

### What was accepted

- Flat layout: all assessment artifacts directly under `DE_AI_C1_Capability_Assessment/`
- Merged README retaining assessment context and full architecture/overview from both versions
- All existing documentation content preserved unchanged (except path references updated)
- Existing `.gitignore` preserved at repository root
- Single `.git` directory at repository root only

### What was changed

- **From:** `DE_AI_C1_Capability_Assessment/databricks-medallion-pipeline/{docs, src, ...}`
- **To:** `DE_AI_C1_Capability_Assessment/{docs, src, ...}`
- Root `README.md` structure diagram updated to show repo root paths
- `requirements-analysis.md` clarification table updated for root-level layout

### What was rejected

- Nested `databricks-medallion-pipeline/` directory (removed)
- Phase 1 data generation (not started)
- CSV generation and pipeline code (not started)
- Git commit (candidate review first)

### Why

The assessment submission should live at the Git repository root so reviewers clone one repo and find the project immediately — no unnecessary nesting. Phase 0 placed files in a subdirectory during initial scaffolding; this correction aligns the repo with the intended assessment structure before Phase 1 begins.

---

## Prompt 4 — Compliance audit (post-implementation)

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-17 |
| **Phase** | Audit (all phases complete) |
| **Activity area** | Submission readiness, documentation consistency, evidence review |

**Prompt summary:** Complete compliance audit after all phases committed (`186d0fd`). Review structure, DQ counts, layer requirements, tests, AI prompt history, debugging evidence, docs consistency, placeholders, security, regression. No features/refactors/commit. Fix clear doc drift only; record outcome without inventing interactions.

**AI response summary:** All implementation phases present; 127 tests pass. Submission gaps: `candidate-info.md`, `reflection.md`, `final-ai-usage-summary.md`, `tool-workflow.md` still Phase 0 templates; `ai-prompts/debugging.md` empty despite nine `debugging-notes.md` issues. Fixed drift in `requirements-analysis.md` and `debugging-notes.md`. No pipeline code changes.

**Outcome:** **COMPLIANT WITH MINOR FIXES** — strong technical evidence; candidate must complete submission metadata and AI reflection. Databricks runtime not validated in repo.

---

## Prompt 5 — Phase 6 submission documentation

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-17 |
| **Phase** | Submission readiness (documentation only) |
| **Activity area** | Candidate templates, reflection, AI summary, checklist |

**Prompt summary:** Final documentation iteration after compliance audit. Prepare `candidate-info.md`, `reflection.md`, `final-ai-usage-summary.md`, `SUBMISSION_CHECKLIST.md`, polish README/tool-workflow/ai-prompts index. No pipeline logic changes. No invented candidate info or fake prompts. No commit/push.

**AI response summary:** Created submission-ready templates with `<TODO: candidate to fill>` placeholders; evidence-based reflection from debugging-notes and phase history; executive AI summary with test progression (86 Silver → 102 Gold → 127 final); `ai-prompts/README.md` clarifying curated vs raw Cursor transcripts; deduplicated `silver-layer.md` duplicate prompt entry; README evaluator quick-start.

**Accepted:** Honest local vs Databricks distinction; curated prompt history terminology; checklist without marking manual tasks complete.

**Changed:** Documentation only — no `src/` or `tests/` modifications.

**Rejected:** Fabricating candidate personal data; inventing Cursor transcripts; claiming Databricks execution.

**Outcome:** **READY FOR CANDIDATE INPUT** — technical repo complete; candidate completes personal fields and optional transcript export before submit.

---

## Prompt 6 — Phase 7 candidate input preparation and evidence review

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-19 |
| **Phase** | Submission readiness (documentation only) |
| **Activity area** | Candidate-info known facts, TODO inventory, evidence review |

**Prompt summary:** After Phase 6, fill only repository-known facts in `candidate-info.md` (Track 1, C1 assessment name, repo name, Cursor Company Account, PySpark/Delta stack, GitHub URL from `git remote`). Classify TODO/placeholder inventory; review reflection and AI summary; prompt-history inventory; git log checkpoints; submission checklist; confirm no `src/`/`tests/` changes. No invented personal data, model versions, or Databricks claims. No commit/push.

**AI response summary:** Updated `candidate-info.md` with assessment track, C1 name, GitHub HTTPS URL from origin remote, Cursor (Company Account), primary implementation stack. Reviewed reflection and `final-ai-usage-summary.md` for factual accuracy. Confirmed curated `ai-prompts/` evidence per phase; six incremental git commits from foundation through dashboard. Verified zero `src/`/`tests/` diff. Regression evidence unchanged at 127 passed.

**Accepted:** GitHub URL from clean HTTPS remote without credentials; manual TODO markers retained for name, designation, model, dates, personalization.

**Changed:** Documentation only — `candidate-info.md`, minor reflection/final-summary/checklist alignment.

**Rejected:** Inventing candidate identity; fabricating Cursor model/version; marking Databricks or manual submission tasks complete.

**Outcome:** **READY FOR MANUAL PERSONALIZATION** — candidate completes personal fields, ratings, reflection voice, optional Cursor export, then commit/push per checklist.

---

## Prompt 7 — Final pre-submission audit

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-19 |
| **Phase** | Final pre-submission audit (documentation and validation only) |
| **Activity area** | Candidate metadata, compliance, hygiene, regression, Gold reconciliation |

### Exact prompt

```text
Perform the FINAL pre-submission audit in /Users/yashikaarora/Desktop/DE_AI_C1_Capability_Assessment. This is audit/final validation only. Do NOT add features, broadly refactor, commit, or push. Make only minimal documentation fixes for clear factual/misleading claims. Do not fabricate Databricks execution.

Established status: all phases complete; previous 127 tests; docs uncommitted.

## 1 Candidate/docs
Review candidate-info.md, reflection.md, final-ai-usage-summary.md, tool-workflow.md, SUBMISSION_CHECKLIST.md, README.md, ai-prompts/README.md and all ai-prompts/*.md. Verify exactly:
- Yashika Arora
- Associate Technical Lead
- To The New
- yashika.arora@tothenew.com
- Assessment ID N/A
- Start Date 17 August 2026 (allow equivalent formatting 17 Aug 2026 but report)
- Submission Date 20 August 2026
- Cursor (Company Account)
- Cursor Auto and Cursor Grok 4.6 (High reasoning effort)
- ratings 3.5,3.5,4,3.5 as specified
- Total Hours absent as required field
Do not rewrite personal narrative absent factual inconsistency.

## 2 TODO audit
Search <TODO, TODO, TBD, PLACEHOLDER. Classify relevant occurrences; descriptive/history references are not blockers. Environment placeholders allowed. Report genuine blockers.

## 3 Requirements compliance
Evidence-check data gen exact counts/determinism; Bronze; Silver; Gold; Dashboard as listed in user request. No new functionality unless required item absent.

## 4 Prompt history
Evidence all areas; curated vs raw distinction; raw export manual if required.

## 5 Hygiene
Non-destructive scans for secrets/token/password/credentials/private keys/local absolute paths; tracked .venv, CSVs, caches, Spark temp, IDE files, unusually large tracked files. Never print secret values; report path/location only. Check .gitignore.

## 6 Git
Run git status; git diff --name-only; git diff --stat; git log --oneline --decorate -10; git remote -v. Confirm only expected docs changed, no src/tests changes, expected GitHub origin, incremental history.

## 7 Full regression
source .venv/bin/activate && pytest tests/ -q. Report exact passes/failures/runtime. If fail, root cause/minimal fix and rerun.

## 8 Gold validation
Run existing scripts/run_gold_validation.py safely. Report actual PASS customers/products/orders, Completed PASS orders, trusted business orders, trusted realized revenue, sales/product revenue, customer revenue, segmentation count. Confirm equality.

## 9 Databricks claims
Search docs for misleading execution claims; separate local validation vs not runtime validated. Minimal docs fix only if needed.

## 10 Checklist
Update supported statuses only; keep manual items unchecked: public visibility unless independently verified, raw Cursor export, final commit/push, Databricks runtime/dashboard execution.

## 11 Prompt history
Record this exact final audit request and actual outcome in ai-prompts/documentation.md as next prompt; curated summary, no fabrication.

Before final report, verify git diff --name-only src/ tests/ empty. Return these 13 sections:
1 Overall readiness
2 Must-fix
3 Should-fix
4 Genuine TODOs
5 Compliance summary
6 Prompt-history
7 Security/hygiene
8 Final regression
9 Gold reconciliation
10 Databricks boundary
11 Manual actions
12 git status
13 git diff --stat
End with exactly one verdict line: READY FOR FINAL COMMIT or NOT READY FOR FINAL COMMIT.

Do not commit/push.
```

### Actual outcome

- Candidate metadata, dates, tool/model details, and ratings matched the requested values; no `Total Hours` field was present.
- Removed stale documentation claims that candidate personalization was still pending; updated only evidence-supported checklist statuses.
- TODO-like occurrences remaining after the audit are historical/descriptive prompt references, not submission blockers.
- Requirements evidence was present for deterministic seed-42 generation, exact issue counts, Bronze ingestion, all four Silver DQ dimensions and metrics, reconciled Gold aggregations, and Gold-only dashboard SQL.
- Prompt history covers planning, all implementation phases, debugging, audits, and submission documentation; it is explicitly curated rather than a raw Cursor export.
- Security and hygiene scans found no credential/private-key patterns, unintended local absolute paths, tracked generated CSVs, virtual environments, caches, Spark temp artifacts, IDE metadata, or tracked files of at least 1 MiB. The exact prompt above necessarily preserves the repository path supplied in the audit request.
- Full regression: `127 passed in 335.14s (0:05:35)`.
- Gold validation: PASS customers 9,940; products 500; orders 99,600; Completed PASS orders 33,042; trusted business orders 32,857; trusted realized revenue 46,978,989.58; product revenue 46,978,989.58; customer revenue 46,978,989.58. Segmentation totals 9,940 customers (High-Value 8,081; Inactive 368; One-Time 1,176; Repeat 315). Product, customer, and trusted revenue were exactly equal.
- Databricks Delta writes, SQL Warehouse execution, and dashboard UI rendering remain explicitly not runtime-validated.
- No `src/` or `tests/` files were modified. No commit or push was performed.

**Outcome:** **READY FOR FINAL COMMIT** after the remaining manual checklist actions.
