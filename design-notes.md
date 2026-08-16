# Design Notes

Architecture and implementation design for the Databricks Medallion e-commerce pipeline. Scoped for a 20–25 hour assessment without unnecessary enterprise complexity.

---

## Pipeline Overview

```text
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │     │   Bronze    │     │   Silver    │     │    Gold     │
│  (CSV files)│ ──► │  (raw Delta)│ ──► │ (validated) │ ──► │ (aggregated)│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │  Dashboard  │
                                                            │  (SQL viz)  │
                                                            └─────────────┘
```

Data flows left to right. Each layer has a single, clear responsibility. Silver is the **quality gate**; Gold never reads directly from Bronze or raw CSVs.

---

## Layer Responsibilities

### Bronze — Raw ingestion

| Responsibility | Detail |
|----------------|--------|
| Read CSVs | Load `customers.csv`, `orders.csv`, `products.csv` from configurable input dir |
| Preserve fidelity | No deduplication, no FK enforcement, no business filtering |
| Schema handling | Explicit PySpark `StructType` per entity (`bronze_common.py`) |
| Type handling | DATE/DECIMAL/INT with documented Spark CSV parse behavior |
| Metadata | `_ingestion_timestamp`, `_source_file`, `_batch_id` on every Bronze row |
| Logging | `bronze_ingestion_log` with row counts per entity per batch |
| Write mode | Entity tables **overwrite**; ingestion log **append** |

**Write strategy trade-off:** Overwrite keeps one current Bronze snapshot per entity for simple dev/reruns. Ingestion log append retains run history. Production deployments may use append-only/immutable Bronze with batch partitioning instead.

**Coordinated runs:** Use `ingest_all.py` so all entities share one `batch_id` and `ingestion_timestamp`. Individual entity scripts generate their own batch id per invocation.

Bronze answers: *"What did we receive, when, and how much?"*

### Silver — Validated, flagged data

| Responsibility | Detail |
|----------------|--------|
| Completeness | Required columns must not be NULL |
| Uniqueness | Primary keys must be unique within entity |
| Referential integrity | Order FKs must exist in customer and product masters |
| Business rules | e.g., `product.price > 0`, valid date formats |
| Flagging | Each row gets `is_valid` and `dq_failure_reasons` |
| Metrics | Aggregate pass/fail counts and percentages per rule per run |

Silver answers: *"Which rows are trustworthy, which failed, and why?"*

### Gold — Business aggregations

| Responsibility | Detail |
|----------------|--------|
| Input | **Only** Silver rows where `is_valid = true` |
| Sales by Product | Quantity and revenue grouped by product |
| Revenue by Customer | Total revenue and order counts per customer |
| Customer Segmentation | Assign segment labels based on revenue tiers |

Gold answers: *"What are the business metrics on clean data?"*

### Dashboard — Consumption

| Responsibility | Detail |
|----------------|--------|
| SQL queries | Against Gold tables (or views) |
| Visualizations | Top products, revenue distribution, segmentation breakdown |
| Audience | Business analysts and data engineers validating pipeline output |

---

## Data Quality Handling

Validation runs in Silver after Bronze ingestion:

1. **Rule catalog** — Each rule has an ID, description, and failure reason code.
2. **Row-level evaluation** — Rules applied per row; multiple failures captured in `dq_failure_reasons`.
3. **No silent deletion** — Invalid rows remain in Silver tables with `is_valid = false`.
4. **Metrics table** — `silver_dq_metrics` (or equivalent) stores per-rule and per-table statistics per pipeline run.
5. **Reporting** — Notebook or SQL view summarizes latest DQ run for human review.

See `data-quality-strategy.md` for the full rule matrix and reason codes.

---

## Invalid Record Flagging

Each Silver entity table includes:

| Column | Type | Purpose |
|--------|------|---------|
| `is_valid` | BOOLEAN | `true` if all rules pass for this row |
| `dq_failure_reasons` | STRING or ARRAY | Codes such as `NULL_EMAIL`, `DUPLICATE_CUSTOMER_ID`, `INVALID_CUSTOMER_ID` |

**Flagging logic:**

- A row with any failed rule → `is_valid = false`
- Reason codes are additive (all failures recorded)
- Duplicate key rows: all rows sharing the duplicate key are flagged
- Invalid FK rows: flagged with `INVALID_CUSTOMER_ID` or `INVALID_PRODUCT_ID` (distinct from NULL codes)

Invalid rows are **excluded from Gold joins and aggregations** but remain queryable for audit and debugging.

---

## How Gold Consumes Validated Data

```text
silver_customers  (is_valid = true)  ──┐
silver_products   (is_valid = true)  ──┼──► Gold aggregations
silver_orders     (is_valid = true)  ──┘
```

**Join strategy (planned):**

- Orders joined to valid products for price and product attributes
- Orders joined to valid customers for customer attributes
- Revenue = `quantity × price`
- Duplicate or invalid orders excluded before aggregation

**Segmentation (planned):**

- Compute per-customer revenue from valid orders
- Assign tiers (e.g., High ≥ 80th percentile, Medium 20–80th, Low < 20th)
- Document exact thresholds in Gold implementation

---

## Testing Strategy

| Layer | Test focus | Approach |
|-------|------------|----------|
| Data generation | Row counts; exact DQ issue counts | `pytest` with fixed random seed |
| Silver validators | Each rule flags expected rows | Small CSV fixtures + assertions |
| Bronze | Metadata columns; row count parity with source | Spark integration test on temp Delta path |
| Gold | Revenue math; grouping; segmentation boundaries | Unit tests with known mini datasets |
| SQL / Dashboard | Queries execute without error | Smoke test after Gold is populated |

**Principles:**

- Test business logic in `src/` modules, not only in notebooks
- Use small fixtures (10–50 rows) for fast local runs
- Full 100k-row pipeline validation on Databricks after integration
- No fabricated test results in documentation until tests are written and run

---

## What We Are Not Building (Scope Boundaries)

- Orchestration platform (Airflow, Dagster)
- Real-time streaming ingestion
- CDC / incremental merge (unless added later with clear justification)
- Multi-environment CI/CD beyond basic test runs
- Custom BI tool integration outside Databricks SQL

These are intentionally excluded to keep scope appropriate for the assessment while maintaining production-quality patterns within each layer.
