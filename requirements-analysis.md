# Requirements Analysis

Assessment requirements distilled into actionable engineering requirements. Based on the Data Engineering AI Capability Assessment specification and planning discussion.

---

## Business Problem

An e-commerce organization needs a **reliable analytics pipeline** that transforms raw operational CSV exports (customers, products, orders) into **trusted business metrics**. Real-world source data contains quality problems—missing values, duplicates, and broken references—that must be detected and handled explicitly rather than silently removed.

The assessment evaluates the ability to design a **Databricks Medallion Architecture** (Bronze → Silver → Gold → Dashboard) using Python, PySpark, and SQL, with production-oriented practices: validation, quarantine, metrics, tests, error handling, and documentation.

**Success criteria:** Clean separation of layers, auditable DQ handling, correct Gold aggregations from validated data, and a dashboard exposing product revenue, customer revenue distribution, and customer segmentation.

---

## Functional Requirements

### Data Generation

| ID | Requirement |
|----|-------------|
| FR-DG-01 | Generate `customers.csv` with **10,000** rows |
| FR-DG-02 | Generate `orders.csv` with **100,000** rows |
| FR-DG-03 | Generate `products.csv` with **500** rows |
| FR-DG-04 | Inject intentional DQ issues at specified counts (see Data Quality section) |
| FR-DG-05 | Data must be realistic (names, dates, categories, prices, quantities) |

### Bronze Layer

| ID | Requirement |
|----|-------------|
| FR-BR-01 | Ingest raw CSV files without business transformations |
| FR-BR-02 | Preserve source data fidelity |
| FR-BR-03 | Handle schemas and data types explicitly |
| FR-BR-04 | Capture ingestion metadata (row counts, ingestion timestamp, source file) |

### Silver Layer

| ID | Requirement |
|----|-------------|
| FR-SL-01 | Validate **completeness** (required fields not null) |
| FR-SL-02 | Validate **uniqueness** (primary key constraints) |
| FR-SL-03 | Validate **referential integrity** (FK references exist in parent tables) |
| FR-SL-04 | Validate **type and business rules** where appropriate |
| FR-SL-05 | Flag bad records; do **not** silently delete |
| FR-SL-06 | Generate DQ metrics: passed/failed counts and pass percentages |

### Gold Layer

| ID | Requirement |
|----|-------------|
| FR-GD-01 | **Sales by Product** aggregation |
| FR-GD-02 | **Revenue by Customer** aggregation |
| FR-GD-03 | **Customer Segmentation** |

### Dashboard

| ID | Requirement |
|----|-------------|
| FR-DB-01 | Top 10 products by revenue |
| FR-DB-02 | Customer revenue distribution |
| FR-DB-03 | Customer segmentation visualization |

### Supporting Deliverables

| ID | Requirement |
|----|-------------|
| FR-SUP-01 | Tests |
| FR-SUP-02 | Error handling |
| FR-SUP-03 | Data-quality reporting |
| FR-SUP-04 | README and setup instructions |
| FR-SUP-05 | Database/schema setup |
| FR-SUP-06 | Documentation, debugging notes, AI prompt history, reflection |

---

## Non-Functional Requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NFR-01 | Reproducibility | Seeded data generation; deterministic validation logic |
| NFR-02 | Maintainability | Modular `src/` layout; config-driven paths and catalog names |
| NFR-03 | Observability | DQ metrics tables; ingestion logs; explicit failure reason codes |
| NFR-04 | Testability | Unit tests on generators, validators, and Gold logic |
| NFR-05 | Performance | Efficient processing of 100k orders on Databricks |
| NFR-06 | Security | No hardcoded credentials; use secrets or env placeholders |
| NFR-07 | Documentation | Architecture, setup, run order, troubleshooting |
| NFR-08 | Scope | Production patterns within ~20–25 hour assessment boundary |

---

## Assumptions

1. **Databricks** is the primary execution environment with **Delta Lake** as the table format.
2. **Unity Catalog** or Hive metastore is available; exact catalog/schema names are environment-specific placeholders.
3. **Revenue** in Gold uses Silver `orders.total_amount` on trusted business orders (sample data sets `total_amount = quantity × unit_price`).
4. **Invalid references** are FK values that do not exist in the respective master table (distinct from NULL FKs).
5. **Customer segmentation** uses rule-based mutually exclusive segments (Inactive, One-Time, Repeat, High-Value) with configurable revenue threshold in Gold.
6. **Dashboard** will use Databricks SQL Dashboard or notebook SQL + charts.
7. **CSV files** are batch-loaded; no streaming or CDC required for this assessment.
8. **Gold layer** consumes only trusted Silver rows (`dq_status = 'PASS'`) and trusted business orders for realized revenue.

---

## Edge Cases

| Edge case | Impact | Planned handling |
|-----------|--------|------------------|
| Duplicate `customer_id` with different attributes | Uniqueness validation | Flag all rows sharing duplicate keys |
| Duplicate `order_id` | Revenue inflation if summed naively | Flag duplicates; Gold dedupes or excludes invalid rows |
| NULL vs invalid FK on orders | Different failure types | Separate reason codes and metric counts |
| Customer with NULL email but valid orders | Completeness vs downstream use | Customer flagged invalid; orders may still reference them |
| Orders referencing invalid customer/product | Referential integrity | Flag order rows; exclude from Gold aggregations |
| Zero or negative product price | Business rule violation | Flag in Silver products validation |
| Customer with no valid orders | Segmentation / revenue | Segment as zero-revenue or exclude; document choice |
| Re-running pipeline | Duplicate Bronze rows | Use overwrite or batch_id strategy (decision in Bronze phase) |
| Empty Gold result after filtering | Dashboard breakage | Queries should handle empty sets gracefully |

---

## Clarifications & Decisions

| Topic | Decision | Rationale |
|-------|----------|-----------|
| Duplicate key handling | Flag all rows involved in a duplicate key violation | Auditable; avoids arbitrary "keep first" without documentation |
| Bad record retention | Retain in Silver with `dq_status = 'FAIL'` and `dq_failure_reasons` | Assessment requires flagging, not silent deletion |
| Gold input | Only validated Silver rows | Ensures business metrics reflect trusted data |
| Storage format | Delta Lake at all Medallion layers | Databricks standard; ACID and schema evolution |
| Segmentation method | Rule-based segments with `GOLD_HIGH_VALUE_THRESHOLD` (default 1000) | Implemented in Gold; dashboard reads `gold_customer_segmentation` |
| Bronze write mode | To be finalized in Bronze phase | Overwrite for simplicity vs append with batch_id for audit |
| Invalid FK generation | IDs outside valid master key ranges | Satisfies "invalid reference" counts separately from NULLs |
| Repository layout | Project artifacts at Git repository root (`DE_AI_C1_Capability_Assessment/`) | Matches assignment structure |
| Dependencies | Minimal until implementation phases | Avoid premature `requirements.txt` bloat |

---

## Intentional Data-Quality Issues (from Assessment)

| Dataset | Issue | Count |
|---------|-------|-------|
| customers | NULL `email` | 50 |
| customers | Duplicate `customer_id` | 10 |
| orders | NULL `customer_id` | 100 |
| orders | NULL `product_id` | 200 |
| orders | Invalid `customer_id` reference | 50 |
| orders | Invalid `product_id` reference | 30 |
| orders | Duplicate `order_id` | 20 |

See `data-quality-strategy.md` for how these map to validation rules and metrics.
