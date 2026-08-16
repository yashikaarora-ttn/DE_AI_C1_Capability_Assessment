# DE AI C1 Capability Assessment — Databricks Medallion Pipeline

Data Engineering AI Capability Assessment repository.

A production-oriented **Databricks Medallion Architecture** pipeline for e-commerce analytics. The pipeline ingests customer, product, and order data through **Bronze → Silver → Gold** layers and exposes business metrics via a **SQL dashboard**.

**Current status: Phase 2 (Bronze ingestion) complete locally.** CSV generator, Bronze PySpark ingestion, and tests are implemented. Silver/Gold/Dashboard planned for subsequent phases.

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
| **Databricks SQL** | Dashboard and ad-hoc queries (planned) |

Environment-specific values (catalog name, schema name, storage paths) will be documented in `database/setup-notes.md` and marked as placeholders in `database/schema.sql`.

---

## Planned Execution Flow

1. **Setup** — Configure catalog/schema; review `database/schema.sql` and setup notes
2. **Data generation** — `python src/data_generation/generate_sample_data.py` ✅
3. **Bronze** — `python src/bronze/ingest_all.py` ✅ (local transform tests; Delta on Databricks)
4. **Silver** — Apply validation rules; flag bad records; publish DQ metrics
5. **Gold** — Build sales-by-product, revenue-by-customer, and customer segmentation tables
6. **Dashboard** — Run SQL queries and build visualizations
7. **Validate** — Run tests; review DQ reports; verify dashboard outputs

Steps 4–7 are **not yet implemented**.

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
| `ai-prompts/` | Prompt history by activity area |

---

## Assessment Context

This repository is part of a **Data Engineering AI Capability Assessment**. Implementation follows a phased plan with production-quality patterns appropriate for a 20–25 hour scope.
