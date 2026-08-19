# Candidate Information

---

## Candidate Details

| Field | Value |
|-------|-------|
| **Candidate Name** | Yashika Arora |
| **Role / Designation** | Associate Technical Lead |
| **Organization** | To The New |
| **Email** | yashika.arora@tothenew.com |

---

## Assessment Details

| Field | Value |
|-------|-------|
| **Assessment Track** | Track 1 — Up to TL Level |
| **Assessment Name** | AI Capability Assessment (C1) — Databricks Medallion Pipeline |
| **Assessment ID / Reference** | N/A |
| **Submission Date** | 20 Aug 2026 |
| **Start Date** | 17 Aug 2026 |

---

## Project & Repository

| Field | Value |
|-------|-------|
| **Project Title** | Databricks Medallion Pipeline — E-Commerce Analytics |
| **Repository Name (local)** | `DE_AI_C1_Capability_Assessment` |
| **GitHub Repository** | https://github.com/yashikaarora-ttn/DE_AI_C1_Capability_Assessment |
| **Primary Branch** | `main` |
| **Latest Commit (at documentation freeze)** | `186d0fd` — Add Databricks dashboard SQL assets and validation tests |

---

## AI Tooling (documented in repository)

| Field | Value |
|-------|-------|
| **AI Tool** | Cursor (Company Account) |
| **AI Models Used** | Cursor Auto; Cursor Grok 4.6 (High reasoning effort) |
| **IDE** | Cursor |
| **Primary Implementation** | PySpark / Spark SQL / Databricks-compatible Delta architecture |
| **Usage Period** | 17 Aug 2026 – 20 Aug 2026 |

Cursor Auto supported general development and routine tasks. Cursor Grok 4.6 with high reasoning effort supported complex coding, debugging, engineering reviews, and validation. Both models were used across the assessment as appropriate; neither was used exclusively.

**Note:** This repository contains **curated phase-wise AI prompt documentation** in `ai-prompts/`, not a complete raw Cursor chat export. If the evaluator requires full conversation transcripts, see `SUBMISSION_CHECKLIST.md` → AI evidence.

---

## Technical Summary (from project — no edit required)

| Item | Status |
|------|--------|
| Data Generation | Complete (seed 42; intentional DQ issues) |
| Bronze | Complete |
| Silver | Complete (4 DQ dimensions; metrics) |
| Gold | Complete (trusted business orders; reconciliation) |
| Dashboard SQL | Complete (Gold-only queries) |
| Local regression tests | **127 passed** (compliance audit) |
| Databricks Delta execution | Not validated in this repository |
| Databricks SQL Dashboard UI | Not validated in this repository |

---

## Declaration

- [x] I confirm this submission represents my own work with AI assistance documented in `ai-prompts/`, `tool-workflow.md`, and `final-ai-usage-summary.md`.
- [x] I have reviewed all generated code and documentation for accuracy.
- [x] I have completed all candidate-specific fields in this file and related submission documents.
- [x] I have verified the GitHub repository is public (if required) and contains no secrets or generated CSVs.

**Signature / Name:** Yashika Arora

**Date:** 20 Aug 2026

---

## Manual Completion Checklist

| Field | Status |
|-------|--------|
| Candidate Name | ☑ |
| Role / Designation | ☑ |
| Organization | ☑ |
| Email | ☑ |
| Assessment ID (if required) | ☑ (N/A) |
| Submission Date | ☑ |
| Start Date | ☑ |
| GitHub Repository URL | ☐ (pre-filled from `git remote`; verify public visibility) |
| AI Model / version | ☑ |
| Usage Period | ☑ |
| Declaration checkboxes | ☑ |
| Signature and date | ☑ |
| Cursor transcript export (if required by evaluator) | ☐ |
