# Submission Checklist

Use this checklist before final submission. **Do not mark items complete unless genuinely done.**

---

## Repository

- [x] Public GitHub repository created/shared
- [x] `main` branch is current with all implementation commits
- [ ] Working tree clean after final commit (`git status` shows nothing uncommitted)
- [x] No secrets, tokens, or credentials found in tracked-file scan
- [x] `README.md` complete and accurate for evaluators
- [x] `SUBMISSION_CHECKLIST.md` reviewed

---

## Candidate Metadata

- [x] `candidate-info.md` — candidate-specific fields completed
- [x] Candidate name entered
- [x] Role / designation entered
- [x] Organization and email entered
- [x] Assessment ID entered as N/A
- [x] GitHub repository URL confirmed (pre-filled in `candidate-info.md` from `git remote` — public visibility verified)
- [x] AI tool confirmed (Cursor Company Account)
- [x] AI models entered (Cursor Auto and Cursor Grok 4.6, High reasoning effort)
- [x] Start and submission dates entered
- [ ] Declaration checkboxes signed
- [x] `reflection.md` personalized
- [x] `final-ai-usage-summary.md` conclusion personalized

---

## Implementation (technical — verify in repo)

- [x] Data Generation — `src/data_generation/` (documented; 17 tests)
- [x] Bronze — `src/bronze/` (documented; 17 tests)
- [x] Silver — `src/silver/` (4 DQ dimensions; metrics; 52+ tests)
- [x] Gold — `src/gold/` (`trusted_business_orders`; reconciliation)
- [x] Dashboard — `src/dashboard/` (Gold-only SQL; 25 tests)

---

## Validation Evidence

- [x] Full regression: **127 tests passed** (compliance audit — rerun before submit if code changed)
- [x] Gold reconciliation: product/customer revenue totals match trusted business orders
- [x] Silver DQ counts on seed-42 data match intentional issue design
- [x] Generated-data counts: 10k customers / 500 products / 100k orders
- [x] Candidate re-ran `pytest tests/ -q` during final pre-submission audit (127 passed)

---

## AI Evidence

- [x] Phase-wise prompt documentation in `ai-prompts/`
- [x] Debugging history: `debugging-notes.md` + `ai-prompts/debugging.md` (Issues 001–009)
- [x] AI usage summary: `final-ai-usage-summary.md`
- [x] Reflection structure: `reflection.md`
- [ ] **Cursor prompt/chat history preserved or exported** — if required by evaluator (see `ai-prompts/README.md`)
  - Repository contains **curated summaries**, not verbatim transcripts

---

## Databricks (honest status)

- [ ] Delta Bronze writes executed on Databricks cluster — **not done in this repo**
- [ ] Delta Silver writes executed on Databricks — **not done in this repo**
- [ ] Delta Gold writes executed on Databricks — **not done in this repo**
- [ ] SQL Warehouse queries executed — **not done in this repo**
- [ ] Dashboard visualizations rendered in Databricks UI — **not done in this repo**

---

## Final Hygiene

- [x] Repository visibility checked (public/private per assessment instructions)
- [ ] GitHub web UI reviewed — no `data/*.csv` committed
- [x] No `.venv/` committed
- [x] No `__pycache__`, `.pytest_cache`, or Spark temp artifacts committed
- [ ] `git status` clean
- [ ] Final commit created with clear message
- [ ] Final push to `origin/main` (or required branch) completed
- [ ] Submission link sent to evaluator (if required)

---

## Quick Reference

| Document | Purpose |
|----------|---------|
| `README.md` | Evaluator entry point |
| `candidate-info.md` | Personal/submission metadata |
| `final-ai-usage-summary.md` | Executive AI usage summary |
| `reflection.md` | Assessment reflection |
| `tool-workflow.md` | AI workflow description |
| `ai-prompts/` | Curated prompt history by phase |
| `debugging-notes.md` | Technical issue log |
