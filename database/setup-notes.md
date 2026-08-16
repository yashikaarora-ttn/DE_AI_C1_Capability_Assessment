# Database Setup Notes

Guidance for configuring the Databricks database environment before running the pipeline. **Not yet executed** — template for upcoming implementation phases.

---

## Overview

The pipeline will use Databricks with **Delta Lake** tables organized in a Medallion layout (Bronze, Silver, Gold). Table DDL reference is in `schema.sql`; actual tables will be created via PySpark writes or SQL during Bronze/Silver/Gold implementation.

---

## Prerequisites

| Item | Notes |
|------|-------|
| Databricks workspace access | Admin or sufficient privileges to create schemas/tables |
| Unity Catalog (recommended) | Or Hive metastore — adjust SQL syntax accordingly |
| Storage location | ADLS Gen2, S3, or DBFS path for Delta files |
| Cluster | Single-user or shared cluster with Delta and PySpark enabled |

---

## Configuration Placeholders

Replace before first pipeline run:

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `${CATALOG_NAME}` | Unity Catalog name | `main` |
| `${SCHEMA_NAME}` | Schema for all pipeline tables | `ecommerce_medallion` |
| `${STORAGE_PATH}` | Root path for Delta table locations | `abfss://...` or `dbfs:/medallion` |

Document your chosen values here when configured:

```text
CATALOG_NAME:   [NOT CONFIGURED]
SCHEMA_NAME:    [NOT CONFIGURED]
STORAGE_PATH:   [NOT CONFIGURED]
```

---

## Setup Steps (Planned)

### Step 1 — Create catalog and schema

```sql
-- Unity Catalog example (customize placeholders)
CREATE CATALOG IF NOT EXISTS <catalog>;
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>
  COMMENT 'E-commerce Medallion pipeline';
```

### Step 2 — Verify storage access

- Confirm cluster can read/write to `${STORAGE_PATH}`
- Test with a small Delta write in a notebook

### Step 3 — Run schema reference (optional)

- Review `schema.sql` for entity definitions
- Execute relevant sections after replacing placeholders
- Or defer to PySpark `saveAsTable` during Bronze ingestion

### Step 4 — Pipeline execution order

1. Generate CSVs (Phase 1)
2. Bronze ingestion → creates Bronze Delta tables
3. Silver validation → creates Silver tables + `silver_dq_metrics`
4. Gold aggregation → creates Gold tables
5. Dashboard → query Gold tables

---

## Unity Catalog vs Hive Metastore

| Feature | Unity Catalog | Hive Metastore |
|---------|---------------|----------------|
| Syntax | `catalog.schema.table` | `schema.table` or `database.table` |
| Governance | Built-in grants | Workspace-level |
| Recommendation | Use if available | Fallback for older workspaces |

Note which mode your workspace uses:

```text
Workspace mode: [UNITY CATALOG / HIVE — NOT CONFIGURED]
```

---

## Troubleshooting (Template)

| Issue | Possible cause | Action |
|-------|----------------|--------|
| `TABLE_OR_VIEW_NOT_FOUND` | Schema not created or wrong catalog | Verify `USE` statement and permissions |
| Storage permission error | Cluster identity lacks write access | Check ADLS RBAC or S3 IAM |
| Delta protocol error | Cluster runtime too old | Upgrade to DBR with Delta 2.x+ |

Add actual issues encountered during setup in `debugging-notes.md`.

---

## Status

| Task | Status |
|------|--------|
| Catalog/schema created | Not done |
| Storage path configured | Not done |
| Reference DDL executed | Not done |
| Bronze tables created | Not done |
