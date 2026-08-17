# AI Prompt History — Gold Layer

Log of AI prompts related to Gold aggregations and customer segmentation (Phase 4).

---

## Prompt 1 — Phase 4 Gold Layer implementation

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-17 |
| **Phase** | Gold |
| **Files affected** | `src/gold/*`, `tests/test_gold_aggregations.py`, docs, `database/schema.sql` |

**Prompt summary:** Implement full Gold layer: trusted Silver consumption policy, four aggregations (SQL + PySpark), configurable threshold, tests, generated-data validation, reconciliation, documentation. No Dashboard. No commit/push.

**AI response summary:** Implemented `gold_common.py` with trusted filtering and `trusted_business_orders` for reconciled realized revenue; four SQL files; `gold_config.py` and `create_gold_tables.py`; 15 Gold tests; full suite 101 passed; seed-42 validation script with reconciliation.

**Accepted:** Trusted business-order policy (PASS Completed + join PASS customer + PASS product); segmentation rules; default threshold 1000; overwrite Gold writes.

**Changed:** Initial product-only join caused revenue reconciliation mismatch; fixed by aligning all Gold metrics on `trusted_business_orders`.

**Rejected:** RFM segmentation; streaming/MERGE; hardcoded validation numbers.

**Why:** User spec required mutual reconciliation and explicit join behavior when entities are individually valid but cannot form trusted business joins.

---

## Gold design reference

### Trusted-data policy

- Entity rows: `dq_status = 'PASS'`
- Realized revenue: `order_status = 'Completed'`
- Business orders: inner join PASS customer and PASS product

### Segmentation (mutually exclusive)

| Segment | Rule |
|---------|------|
| Inactive | 0 business orders |
| One-Time | 1 business order |
| High-Value | >1 orders AND revenue ≥ `GOLD_HIGH_VALUE_THRESHOLD` |
| Repeat | >1 orders AND revenue < threshold |

Default threshold: **1000** (`GOLD_HIGH_VALUE_THRESHOLD`).

### Tables

- `gold_sales_by_product`
- `gold_revenue_by_customer`
- `gold_daily_weekly_trends`
- `gold_customer_segmentation`

### Local vs Databricks

- **Local:** PySpark aggregations, pytest, `scripts/run_gold_validation.py`
- **Databricks:** `create_gold_tables.py` Delta writes — not integration-tested in repo
