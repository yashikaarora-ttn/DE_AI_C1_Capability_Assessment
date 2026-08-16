# DE AI C1 Capability Assessment — Databricks Medallion Pipeline

Data Engineering AI Capability Assessment repository.

A production-oriented **Databricks Medallion Architecture** pipeline for e-commerce analytics. The pipeline ingests customer, product, and order data through **Bronze → Silver → Gold** layers and exposes business metrics via a **SQL dashboard**.

**Current status: Foundation phase (Phase 0).** Repository structure and documentation are in place. Data generation, Bronze/Silver/Gold pipeline code, tests, and dashboard implementation are planned for subsequent phases.

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
├── data/                    # Generated CSVs (later phases)
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
| **Python 3.10+** | Local development and testing (planned) |
| **PySpark** | Available on Databricks cluster; local optional for tests |
| **Delta Lake** | Storage format for Bronze/Silver/Gold tables |
| **Databricks SQL** | Dashboard and ad-hoc queries (planned) |

Environment-specific values (catalog name, schema name, storage paths) will be documented in `database/setup-notes.md` and marked as placeholders in `database/schema.sql`.

---

## Planned Execution Flow

1. **Setup** — Configure catalog/schema; review `database/schema.sql` and setup notes
2. **Data generation** — Produce `customers.csv`, `orders.csv`, `products.csv` with intentional DQ issues
3. **Bronze** — Ingest raw CSVs to Delta; log ingestion metadata
4. **Silver** — Apply validation rules; flag bad records; publish DQ metrics
5. **Gold** — Build sales-by-product, revenue-by-customer, and customer segmentation tables
6. **Dashboard** — Run SQL queries and build visualizations
7. **Validate** — Run tests; review DQ reports; verify dashboard outputs

Steps 2–7 are **not yet implemented**.

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
