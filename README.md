# DE AI C1 Capability Assessment — Databricks Medallion Pipeline

Data Engineering AI Capability Assessment repository.

A production-oriented **Databricks Medallion Architecture** pipeline for e-commerce analytics. The pipeline ingests customer, product, and order data through **Bronze → Silver → Gold** layers and exposes business metrics via a **SQL dashboard**.

**Current status: All implementation phases complete.** **127 regression tests** passing (final pre-submission audit). Submission documentation and candidate metadata are complete; manual submission actions remain.

---

## Quick Start for Evaluators

| Question | Where to look |
|----------|---------------|
| What does this project do? | [Architecture Overview](#architecture-overview) below |
| How is data generated and what DQ issues exist? | [Sample Data Generation](#sample-data-generation-phase-1) |
| What does each layer do? | Bronze, Silver, Gold, Dashboard sections below |
| How do I run locally? | `pip install -r requirements.txt` → generate CSVs → `pytest tests/ -q` |
| How do I run on Databricks? | `database/setup-notes.md`; layer `create_*_tables.py` scripts |
| What is locally validated vs not? | [Validation Status](#validation-status) |
| Where is AI prompt history? | `ai-prompts/` (curated summaries; see `ai-prompts/README.md`) |
| Submission checklist? | `SUBMISSION_CHECKLIST.md` |

### Validation Status

| Locally validated | Not validated in this repository |
|-------------------|----------------------------------|
| PySpark transforms (Bronze/Silver/Gold) | Databricks Delta writes |
| 127 pytest regression tests | SQL Warehouse query execution |
| Seed-42 DQ counts and Gold reconciliation | Dashboard UI rendering |
| Dashboard SQL static contracts (25 tests) | |

### Run full test suite

```bash
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -q   # 127 tests at last audit
```

---

## Sample Data Generation (Phase 1)

### Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Requires Python 3.10+. On macOS with an externally managed system Python, use a virtual environment as shown above.

### Generate CSV files

From the repository root:

```bash
python src/data_generation/generate_sample_data.py
```

Options:

```bash
python src/data_generation/generate_sample_data.py --seed 42 --output-dir data
```

### Output files

| File | Rows | Location |
|------|------|----------|
| `customers.csv` | 10,000 | `data/customers.csv` |
| `products.csv` | 500 | `data/products.csv` |
| `orders.csv` | 100,000 | `data/orders.csv` |

Generated CSVs are **gitignored**.

### Intentional data-quality issues

| Dataset | Issue | Count |
|---------|-------|-------|
| customers | NULL `email` | 50 |
| customers | Duplicate `customer_id` rows | 10 (5 ids × 2 rows) |
| orders | NULL `customer_id` | 100 |
| orders | NULL `product_id` | 200 |
| orders | Invalid `customer_id` (non-null) | 50 |
| orders | Invalid `product_id` (non-null) | 30 |
| orders | Duplicate `order_id` rows | 20 (10 ids × 2 rows) |

See `src/data_generation/DATA_GENERATION_NOTES.md` for duplicate row definitions and disjoint issue categories.

### Reproducibility

Default seed is `42`. The same seed produces identical output (`random` + `Faker` seeded).

### Run tests

```bash
pytest tests/test_data_generation.py -v
```

---

## Bronze Ingestion (Phase 2)

Ingest raw CSVs to Delta Bronze tables with explicit schemas and ingestion metadata.

### Environment variables (optional)

| Variable | Default | Purpose |
|----------|---------|---------|
| `BRONZE_CATALOG` | unset | Unity Catalog name (e.g. `main`) |
| `BRONZE_SCHEMA` | `ecommerce_medallion` | Schema/database name |
| `BRONZE_STORAGE_PATH` | unset | Root storage path for Delta files |
| `BRONZE_INPUT_DIR` | `data` | Directory containing CSV files (repo-relative) |
| `BRONZE_ENTITY_WRITE_MODE` | `overwrite` | Bronze entity table write mode |
| `BRONZE_LOG_WRITE_MODE` | `append` | Ingestion log write mode |

### Run locally (requires Java for PySpark)

```bash
source .venv/bin/activate
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
python src/bronze/ingest_all.py
```

Individual entity scripts:

```bash
python src/bronze/01_ingest_customers.py
python src/bronze/02_ingest_orders.py
python src/bronze/03_ingest_products.py
```

### Databricks execution

1. Upload/sync repository to Databricks workspace or Repos.
2. Generate CSVs on cluster or upload `data/*.csv` to DBFS/cloud path.
3. Set environment variables on the cluster/job (catalog, schema, storage path, input dir).
4. Run as a Python job or notebook:

```python
%run ./src/bronze/ingest_all.py
```

Or from a job task: `python src/bronze/ingest_all.py` with `PYTHONPATH=src`.

**Delta write validation** is an integration step on Databricks (local tests cover read/transform/metadata without requiring Delta Lake).

### Bronze tables

| Table | Source CSV | Write mode |
|-------|------------|------------|
| `bronze_customers` | `customers.csv` | overwrite |
| `bronze_orders` | `orders.csv` | overwrite |
| `bronze_products` | `products.csv` | overwrite |
| `bronze_ingestion_log` | — | append |

### Run tests

```bash
pytest tests/test_data_generation.py tests/test_bronze_ingestion.py -v
```

---

## Silver Validation (Phase 3)

Silver establishes trusted types and explicit row-level DQ results without deleting failed rows.

### Validation order

1. Completeness (`01_quality_completeness.py`)
2. Type / business rules (`03_quality_type_validation.py`)
3. Uniqueness (`02_quality_uniqueness.py`)
4. Referential integrity (`04_quality_referential_integrity.py`)

Orchestration: `silver_foundation.apply_silver_pipeline()` or `apply_silver_all()`.

### Implemented modules

| Module | Purpose |
|--------|---------|
| `src/silver/silver_common.py` | Reason codes, FK normalization, reason accumulation |
| `src/silver/01_quality_completeness.py` | Required-field completeness |
| `src/silver/02_quality_uniqueness.py` | PK uniqueness (all rows in duplicate groups flagged) |
| `src/silver/03_quality_type_validation.py` | FK STRING→INTEGER, type and business rules |
| `src/silver/04_quality_referential_integrity.py` | Order FK references vs customers/products |
| `src/silver/05_quality_business_logic.py` | Thin delegate to type/business rules (assignment alignment) |
| `src/silver/dq_metrics.py` | DQ metrics builder (RULE + OVERALL) |
| `src/silver/silver_config.py` | Silver config and Delta write helpers |
| `src/silver/create_silver_tables.py` | Full pipeline + optional Delta writes |

**Not yet:** Databricks SQL Warehouse execution and dashboard rendering.

---

## Dashboard (Phase 5)

SQL assets in `src/dashboard/` consume **Gold tables only** (no Bronze/Silver).

| File | Visualization | Gold source | Chart |
|------|---------------|-------------|-------|
| `01_top_10_products_by_revenue.sql` | Top 10 products | `gold_sales_by_product` | Bar |
| `02_customer_revenue_distribution.sql` | Revenue bands | `gold_revenue_by_customer` | Bar/column |
| `03_customer_segmentation.sql` | Segmentation | `gold_customer_segmentation` | Pie/donut or bar |
| `04_revenue_trend.sql` | Weekly revenue trend | `gold_daily_weekly_trends` | Line |

Setup guide: `src/dashboard/dashboard_setup.md`

### Revenue distribution bands (presentation assumptions)

| Band | `total_revenue` rule |
|------|----------------------|
| No Revenue | = 0 |
| Low | > 0 and < 500 |
| Medium | >= 500 and < 2000 |
| High | >= 2000 and < 5000 |
| Very High | >= 5000 |

Bands are defined in `02_customer_revenue_distribution.sql` (dashboard presentation assumptions for seed-42 scale data).

### Run dashboard tests

```bash
pytest tests/test_dashboard_queries.py -v
```

**Locally validated:** SQL file contracts, Gold-only references, lightweight PySpark checks against Gold outputs.

**Not yet validated:** Databricks SQL execution, SQL Warehouse, actual dashboard UI.

---

## Gold Aggregations (Phase 4)

Gold consumes **only trusted Silver data** and computes business-ready metrics.

### Trusted-data policy

| Rule | Detail |
|------|--------|
| Entity trust | `dq_status = 'PASS'` on customers, products, and orders |
| Realized revenue | `order_status = 'Completed'` only |
| Business orders | PASS Completed orders that join PASS customer **and** PASS product |

A PASS order is excluded from Gold realized metrics when its customer or product failed Silver (cannot form a trusted business join). Failed Silver rows (NULL FK, invalid FK, duplicates, etc.) never influence Gold metrics.

### Aggregations

| Table | Module | Grain |
|-------|--------|-------|
| `gold_sales_by_product` | `01_sales_by_product.sql`, `gold_common.build_sales_by_product` | Per product with qualifying orders |
| `gold_revenue_by_customer` | `02_revenue_by_customer.sql`, `build_revenue_by_customer` | Per PASS customer (zeros if no business orders) |
| `gold_daily_weekly_trends` | `03_daily_weekly_trends.sql`, `build_daily_weekly_trends` | Daily + weekly periods |
| `gold_customer_segmentation` | `04_customer_segmentation.sql`, `build_customer_segmentation` | Per segment type |

### Segmentation (mutually exclusive)

Evaluated on trusted business orders per PASS customer:

| Segment | Rule |
|---------|------|
| `Inactive` | Zero trusted completed business orders |
| `One-Time` | Exactly one trusted completed business order |
| `High-Value` | More than one order AND `total_revenue >= HIGH_VALUE_THRESHOLD` |
| `Repeat` | More than one order AND revenue below threshold |

Default `GOLD_HIGH_VALUE_THRESHOLD` = **1000** (env-configurable).

`lifetime_value_actual` in `gold_revenue_by_customer` is **observed** trusted revenue from orders; source `lifetime_value` on customers is not used for Gold metrics.

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `GOLD_CATALOG` | falls back to Silver/Bronze catalog | Unity Catalog |
| `GOLD_SCHEMA` | Silver/Bronze schema | Schema name |
| `GOLD_STORAGE_PATH` | Silver/Bronze storage path | Delta location |
| `GOLD_HIGH_VALUE_THRESHOLD` | `1000` | High-Value segmentation threshold |
| `GOLD_WRITE_MODE` | `overwrite` | Gold snapshot write mode |

### Run Gold

```bash
pytest tests/test_gold_aggregations.py -v
python src/gold/create_gold_tables.py
python scripts/run_gold_validation.py   # generated-data report
```

### Reconciliation

Product-level and customer-level `total_revenue` sums both equal the trusted business-order revenue total. Segmentation `customer_count` sums reconcile to PASS customer count.

**Locally validated:** PySpark aggregations, SQL definition files (not executed on Databricks), unit/integration tests, seed-42 reconciliation via `scripts/run_gold_validation.py`.

**Not yet validated in Databricks:** Gold Delta writes, Gold SQL execution, Dashboard SQL Warehouse execution and UI rendering.

---

## Silver Validation (Phase 3)

### Silver write strategy

| Table | Mode | Rationale |
|-------|------|-----------|
| `silver_customers`, `silver_orders`, `silver_products` | **overwrite** | Latest validated snapshot (matches Bronze entity strategy) |
| `silver_dq_metrics` | **append** | Metrics history per batch/run |

### Run Silver

```bash
# Full test suite
pytest tests/ -v

# Create Silver tables (Databricks with Delta)
python src/silver/create_silver_tables.py
```

### Silver row flags

| Column | Description |
|--------|-------------|
| `dq_status` | `PASS` or `FAIL` |
| `dq_failure_reasons` | `ARRAY<STRING>` of stable reason codes |
| `_silver_processed_at` | Validation timestamp |

Failed rows remain in Silver. Multiple reason codes accumulate per row.

### Run tests

```bash
pytest tests/test_data_generation.py tests/test_bronze_ingestion.py \
  tests/test_silver_completeness.py tests/test_silver_type_validation.py \
  tests/test_silver_uniqueness.py tests/test_silver_referential_integrity.py \
  tests/test_silver_full_pipeline.py tests/test_silver_dq_metrics.py \
  tests/test_silver_table_creation.py -v
```

---

## Architecture Overview

```text
CSV Sources          Bronze (raw)         Silver (validated)      Gold (aggregated)     Dashboard
─────────────        ─────────────        ──────────────────      ─────────────────     ─────────
customers.csv  ──►   bronze_customers ──► silver_customers  ──► sales_by_product  ──► Top products
orders.csv     ──►   bronze_orders    ──► silver_orders     ──► revenue_by_customer ─► Revenue dist.
products.csv   ──►   bronze_products  ──► silver_products   ──► customer_segmentation ► Segmentation
```

| Layer | Responsibility |
|-------|----------------|
| **Bronze** | Ingest raw CSVs; preserve source fidelity; capture schemas, types, and ingestion metadata |
| **Silver** | Validate completeness, uniqueness, referential integrity, and business rules; flag invalid rows |
| **Gold** | Business-ready aggregations from validated Silver data |
| **Dashboard** | SQL queries and visualizations for product revenue, customer revenue, and segmentation |

See `design-notes.md` and `data-quality-strategy.md` for detailed design decisions.

---

## Repository Structure

```text
DE_AI_C1_Capability_Assessment/
├── README.md
├── SUBMISSION_CHECKLIST.md
├── candidate-info.md
├── tool-workflow.md
├── requirements-analysis.md
├── design-notes.md
├── data-model.md
├── data-quality-strategy.md
├── debugging-notes.md
├── reflection.md
├── final-ai-usage-summary.md
├── src/
│   ├── data_generation/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── dashboard/
├── data/                    # Generated CSVs (gitignored)
├── requirements.txt         # Python deps for data generation & tests
├── database/
│   ├── schema.sql
│   ├── setup-notes.md
│   └── seed-data-notes.md
├── tests/
└── ai-prompts/
```

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Databricks workspace** | Runtime for PySpark pipeline execution |
| **Python 3.10+** | Local data generation and pytest |
| **PySpark** | Available on Databricks cluster; local optional for tests |
| **Delta Lake** | Storage format for Bronze/Silver/Gold tables |
| **Databricks SQL** | Dashboard SQL assets prepared; UI execution on cluster |

Environment-specific values (catalog name, schema name, storage paths) will be documented in `database/setup-notes.md` and marked as placeholders in `database/schema.sql`.

---

## Planned Execution Flow

1. **Setup** — Configure catalog/schema; review `database/schema.sql` and setup notes
2. **Data generation** — `python src/data_generation/generate_sample_data.py` ✅
3. **Bronze** — `python src/bronze/ingest_all.py` ✅ (local transform tests; Delta on Databricks)
4. **Silver** — Full validation pipeline ✅; DQ metrics ✅
5. **Gold** — Build sales-by-product, revenue-by-customer, trends, and segmentation ✅
6. **Dashboard** — SQL queries and Databricks dashboard setup ✅ (local assets)
7. **Validate** — Run tests; review DQ reports; verify dashboard on Databricks

Step 7 Databricks dashboard verification is **not yet executed in this repo**.

---

## Documentation Index

| Document | Purpose |
|----------|---------|
| `requirements-analysis.md` | Business problem, functional/non-functional requirements, assumptions |
| `design-notes.md` | Layer responsibilities, DQ handling, testing strategy |
| `data-model.md` | Entity schemas, keys, relationships |
| `data-quality-strategy.md` | Validation rules, reason codes, metrics |
| `database/setup-notes.md` | Database/catalog setup guidance |
| `database/seed-data-notes.md` | Sample data generation plan |
| `tool-workflow.md` | AI-assisted development workflow |
| `final-ai-usage-summary.md` | Executive AI usage summary |
| `reflection.md` | Assessment reflection |
| `SUBMISSION_CHECKLIST.md` | Pre-submission checklist |
| `ai-prompts/` | Curated prompt history by activity area |

---

## Assessment Context

This repository is part of a **Data Engineering AI Capability Assessment**. Implementation follows a phased plan with production-quality patterns appropriate for a 20–25 hour scope.
