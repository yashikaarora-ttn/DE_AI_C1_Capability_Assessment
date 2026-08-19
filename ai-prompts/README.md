# AI Prompt History — Index

This folder contains **curated, phase-wise documentation** of AI-assisted development for the Data Engineering AI Capability Assessment.

---

## What This Folder Contains

| Content | Description |
|---------|-------------|
| **Prompt summaries** | What was asked, what AI produced, what was accepted/rejected, and why |
| **Phase decisions** | Engineering choices documented per medallion layer |
| **Debugging evidence** | Issue-driven summaries linked to `debugging-notes.md` |

Files are written for **evaluator readability** and audit trail — not as a byte-for-byte Cursor chat export.

---

## What This Folder Does NOT Contain

- Complete verbatim Cursor conversation transcripts for every session
- Automatic export of all IDE chat turns

If the assessment or evaluator requires **full raw Cursor history**, the candidate should separately:

1. Export or preserve Cursor chat sessions from the project period, or
2. Provide screenshots/links per evaluator instructions

See `SUBMISSION_CHECKLIST.md` → AI Evidence.

---

## File Index

| File | Phase / topic |
|------|----------------|
| `documentation.md` | Requirements, foundation, compliance audits, submission docs |
| `data-generation.md` | Phase 1 sample data |
| `bronze-layer.md` | Phase 2 Bronze ingestion |
| `silver-layer.md` | Phase 3 Silver DQ |
| `gold-layer.md` | Phase 4 Gold aggregations |
| `dashboard.md` | Phase 5 Dashboard SQL |
| `debugging.md` | Issues 001–009 summaries |

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `tool-workflow.md` | End-to-end AI workflow description |
| `final-ai-usage-summary.md` | Executive AI usage summary |
| `debugging-notes.md` | Technical issue log with root causes |
| `reflection.md` | Assessment reflection |

---

## Cursor Usage

**Primary AI tool:** Cursor (Company Account), as documented in `tool-workflow.md` and `candidate-info.md`.

**Models used:** Cursor Auto for general development and routine tasks; Cursor Grok 4.6 with high reasoning effort for complex coding, debugging, engineering reviews, and validation. Neither model was used exclusively.
