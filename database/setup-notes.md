# Database Setup Notes

Guidance for configuring the Databricks environment and running Bronze ingestion.

---

## Overview

The pipeline uses **Delta Lake** tables in a Medallion layout. Bronze ingestion is implemented in `src/bronze/` and creates:

- `bronze_customers`, `bronze_orders`, `bronze_products`
- `bronze_ingestion_log`

DDL reference: `database/schema.sql`

---

## Prerequisites

| Item | Notes |
|------|-------|
| Databricks workspace | With Delta-enabled cluster (DBR 13+ recommended) |
| Unity Catalog (recommended) | Or Hive metastore |
| Storage | ADLS Gen2, S3, or DBFS path for Delta |
| Sample CSVs | From Phase 1 generator in `data/` or workspace path |

---

## Configuration

Set on cluster or job:

| Variable | Example | Required |
|----------|---------|----------|
| `BRONZE_CATALOG` | `main` | Recommended (Unity Catalog) |
| `BRONZE_SCHEMA` | `ecommerce_medallion` | Yes |
| `BRONZE_STORAGE_PATH` | `abfss://.../medallion` or `dbfs:/medallion` | Recommended |
| `BRONZE_INPUT_DIR` | `/Workspace/Repos/.../data` | Yes (path to CSVs) |

Document your values when configured:

```text
CATALOG_NAME:   [NOT CONFIGURED]
SCHEMA_NAME:    ecommerce_medallion (default)
STORAGE_PATH:   [NOT CONFIGURED]
INPUT_DIR:      data (default, repo-relative locally)
```

---

## Setup Steps

### Step 1 — Create catalog and schema

```sql
CREATE CATALOG IF NOT EXISTS <catalog>;
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>
  COMMENT 'E-commerce Medallion pipeline';
```

### Step 2 — Generate or upload CSVs

```bash
python src/data_generation/generate_sample_data.py
```

Upload to Databricks if running remotely.

### Step 3 — Run Bronze ingestion

```bash
export BRONZE_CATALOG=main
export BRONZE_SCHEMA=ecommerce_medallion
export BRONZE_STORAGE_PATH=dbfs:/tmp/medallion_assessment
export BRONZE_INPUT_DIR=data
python src/bronze/ingest_all.py
```

### Step 4 — Verify (Databricks)

```sql
SELECT COUNT(*) FROM <catalog>.<schema>.bronze_customers;  -- expect 10000
SELECT COUNT(*) FROM <catalog>.<schema>.bronze_orders;     -- expect 100000
SELECT COUNT(*) FROM <catalog>.<schema>.bronze_products;   -- expect 500
SELECT * FROM <catalog>.<schema>.bronze_ingestion_log ORDER BY ingestion_timestamp DESC;
```

**Databricks execution not yet validated in this repo** — verify after first cluster run.

---

## Pipeline execution order

1. Generate CSVs (Phase 1) ✅
2. Bronze ingestion (Phase 2) ✅
3. Silver validation + metrics (Phase 3) ✅ code ready
4. Gold aggregation (planned)
5. Dashboard (planned)

### Silver environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SILVER_CATALOG` | `BRONZE_CATALOG` | Unity Catalog name |
| `SILVER_SCHEMA` | `BRONZE_SCHEMA` / `ecommerce_medallion` | Schema name |
| `SILVER_STORAGE_PATH` | `BRONZE_STORAGE_PATH` | Delta root path |
| `SILVER_ENTITY_WRITE_MODE` | `overwrite` | Silver entity tables |
| `SILVER_METRICS_WRITE_MODE` | `append` | `silver_dq_metrics` |
| `SILVER_READ_BRONZE_DELTA` | `false` | Read Bronze from Delta vs CSV prepare |

### Run Silver locally (transform + metrics, no Delta)

```bash
python -c "
from silver.create_silver_tables import run_silver_pipeline
from bronze.bronze_common import BronzeConfig, get_spark_session
from silver.silver_config import SilverConfig
spark = get_spark_session('silver-local')
run_silver_pipeline(spark, BronzeConfig.from_env(), SilverConfig.from_env(), write_delta=False)
"
```

### Run Silver on Databricks (Delta writes)

```bash
export SILVER_CATALOG=main
export SILVER_SCHEMA=ecommerce_medallion
export SILVER_STORAGE_PATH=dbfs:/tmp/medallion_assessment
export BRONZE_INPUT_DIR=/path/to/data
python src/silver/create_silver_tables.py
```

**Databricks Silver Delta integration not yet validated in this repo** — local pipeline and metrics are tested; Delta write path mirrors Bronze.

---

## Status

| Task | Status |
|------|--------|
| Catalog/schema DDL documented | Done |
| Bronze PySpark scripts | Done |
| Silver validation + metrics | Done |
| Local Silver transform/metrics tests | Done |
| Databricks Delta integration run (Bronze/Silver) | Not executed in repo |
