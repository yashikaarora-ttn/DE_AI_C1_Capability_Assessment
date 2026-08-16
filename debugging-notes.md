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
| Local Python version | `[NOT CONFIGURED]` |

---

## Issue Log

### Issue 001 — (Template)

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
| Issues logged | 0 |
| Open issues | 0 |
| Resolved issues | 0 |
