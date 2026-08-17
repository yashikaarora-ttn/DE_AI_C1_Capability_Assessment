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
| Flagging | Each row gets `dq_status` (`PASS`/`FAIL`) and `dq_failure_reasons` array |
| Metrics | `silver_dq_metrics` — RULE per reason + OVERALL per entity (**implemented**) |

**Phase 4 (complete):** Gold aggregations (`gold_common.py`, `create_gold_tables.py`, SQL definitions), trusted business-order policy, reconciliation tests. Dashboard remains.

Silver answers: *"Which rows are trustworthy, which failed, and why?"*

### Gold — Business aggregations

| Responsibility | Detail |
|----------------|--------|
| Input | PASS Silver entities; realized metrics from **trusted business orders** |
| Trusted business order | PASS + `Completed` + joins PASS customer and PASS product |
| Sales by Product | `total_orders`, `total_revenue`, `avg_order_value` per product |
| Revenue by Customer | Per PASS customer; `lifetime_value_actual` from observed orders |
| Customer Segmentation | Mutually exclusive: Inactive, One-Time, Repeat, High-Value |
| Trends | Daily and weekly `total_orders`, revenue, AOV |

Gold answers: *"What are the business metrics on clean, joinable data?"*

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
3. **No silent deletion** — Invalid rows remain in Silver tables with `dq_status = 'FAIL'`.
4. **Metrics table** — `silver_dq_metrics` (or equivalent) stores per-rule and per-table statistics per pipeline run.
5. **Reporting** — Notebook or SQL view summarizes latest DQ run for human review.

See `data-quality-strategy.md` for the full rule matrix and reason codes.

---

## Invalid Record Flagging

Each Silver entity table includes:

| Column | Type | Purpose |
|--------|------|---------|
| `dq_status` | STRING | `PASS` or `FAIL` |
| `dq_failure_reasons` | ARRAY&lt;STRING&gt; | Codes such as `NULL_EMAIL`, `DUPLICATE_CUSTOMER_ID`, `INVALID_CUSTOMER_ID` |

**Flagging logic:**

- A row with any failed rule → `dq_status = FAIL`
- Reason codes are additive (all failures recorded)
- Duplicate key rows: all rows sharing the duplicate key are flagged
- Invalid FK rows: flagged with `INVALID_CUSTOMER_ID` or `INVALID_PRODUCT_ID` (distinct from NULL codes)

Invalid rows are **excluded from Gold joins and aggregations** but remain queryable for audit and debugging.

---

## How Gold Consumes Validated Data

```text
silver_customers  (dq_status = 'PASS')  ──┐
silver_products   (dq_status = 'PASS')  ──┼──► trusted_business_orders
silver_orders     (dq_status = 'PASS', order_status = 'Completed') ──┘
         │
         ├──► gold_sales_by_product
         ├──► gold_revenue_by_customer
         ├──► gold_daily_weekly_trends
         └──► gold_customer_segmentation
```

**Join strategy (implemented):**

- Trusted business orders: inner join PASS customer and PASS product on order FKs
- PASS orders referencing FAIL customers/products are excluded from realized Gold metrics
- Revenue uses Silver `total_amount` on trusted business orders (not recomputed from price × quantity)
- Duplicate, NULL FK, and invalid FK orders excluded via Silver `dq_status = FAIL`

**Segmentation (implemented):**

| Segment | Rule |
|---------|------|
| Inactive | 0 trusted business orders |
| One-Time | 1 trusted business order |
| High-Value | >1 orders AND revenue ≥ `GOLD_HIGH_VALUE_THRESHOLD` (default 1000) |
| Repeat | >1 orders AND revenue below threshold |

**Reconciliation:** Sum of `total_revenue` across products equals sum across customers equals total trusted business-order revenue.

---

## Testing Strategy

| Layer | Test focus | Approach |
|-------|------------|----------|
| Data generation | Row counts; exact DQ issue counts | `pytest` with fixed random seed |
| Silver validators | Each rule flags expected rows | Small CSV fixtures + assertions |
| Bronze | Metadata columns; row count parity with source | Spark integration test on temp Delta path |
| Gold | Revenue math; grouping; segmentation; reconciliation | `test_gold_aggregations.py` + seed-42 e2e |
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
