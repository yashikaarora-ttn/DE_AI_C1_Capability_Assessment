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

## Future prompts

Add new entries below as documentation and planning prompts occur.

### Prompt N — (Template)

| Field | Detail |
|-------|--------|
| **Date** | |
| **Phase** | |
| **Activity area** | |

**Prompt summary:**  
**AI response summary:**  
**Accepted / Changed / Rejected / Why:**
