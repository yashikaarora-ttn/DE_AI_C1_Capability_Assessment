# AI Prompt History — Dashboard

Log of AI prompts related to SQL queries, visualizations, and Databricks SQL Dashboard (Phase 5).

---

## Prompt 1 — Phase 5 Dashboard implementation

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-17 |
| **Phase** | Dashboard |
| **Files affected** | `src/dashboard/*`, `tests/test_dashboard_queries.py`, docs |

**Prompt summary:** Implement Databricks SQL dashboard assets consuming Gold tables only: Top 10 products, revenue distribution bands, segmentation, weekly trend; setup guide; static validation tests; documentation. No Bronze/Silver/Gold logic changes. No commit/push.

**AI response summary:** Created four SQL files + `dashboard_setup.md`; revenue bands (No Revenue / Low / Medium / High / Very High) on `gold_revenue_by_customer.total_revenue`; segmentation and trend read pre-aggregated Gold tables; 22 static + lightweight PySpark tests; docs updated.

**Accepted:** Gold-only consumption; weekly trend; bar/line/pie chart recommendations; assessment-scale band thresholds (500 / 2000 / 5000).

**Changed:** README band table aligned to SQL thresholds (not 10000 placeholder version).

**Rejected:** Python dashboard frameworks; Silver/Bronze dashboard queries; reimplemented segmentation in SQL.

**Why:** Assignment requires SQL dashboard assets on existing Gold outputs without altering medallion business logic.

---

## Dashboard assets reference

| File | Gold table | Chart |
|------|------------|-------|
| `01_top_10_products_by_revenue.sql` | `gold_sales_by_product` | Bar |
| `02_customer_revenue_distribution.sql` | `gold_revenue_by_customer` | Bar/column |
| `03_customer_segmentation.sql` | `gold_customer_segmentation` | Pie/donut or bar |
| `04_revenue_trend.sql` | `gold_daily_weekly_trends` (WEEKLY) | Line |

### Revenue bands (presentation assumptions)

| Band | Rule |
|------|------|
| No Revenue | `total_revenue = 0` |
| Low | `> 0` and `< 500` |
| Medium | `>= 500` and `< 2000` |
| High | `>= 2000` and `< 5000` |
| Very High | `>= 5000` |

### Validation status

- **Local:** SQL contracts, Gold-only references, PySpark shape checks (25 dashboard tests; 127 full regression)
- **Not validated:** Databricks SQL Warehouse execution, dashboard UI rendering

---

## Prompt 2 — Phase 5 Dashboard pre-commit engineering review

| Field | Detail |
|-------|--------|
| **Date** | 2026-08-17 |
| **Phase** | Dashboard |
| **Files affected** | `01_top_10_products_by_revenue.sql`, `tests/test_dashboard_queries.py`, `ai-prompts/dashboard.md` |

**Prompt summary:** Pre-commit review of dashboard SQL, tests, docs, and Gold contract alignment. Verify Gold-only consumption, four required visualizations, band/segmentation/trend correctness, static tests, run regression. No Bronze/Silver/Gold logic changes. No commit/push.

**AI response summary:** Review confirmed Gold-only dashboard SQL for all four visualizations; revenue bands mutually exclusive and documented as presentation assumptions; segmentation pass-through only; weekly trend from pre-aggregated Gold. Added `product_id ASC` tie-break on Top 10 ordering and test. All 25 dashboard + 127 regression tests passed.

**Accepted:** Weekly trend grain; band thresholds 500/2000/5000 for seed-42; `${schema}` placeholders; static vs Databricks validation split.

**Changed:** Top 10 `ORDER BY` now includes `product_id ASC` for deterministic tie-breaking; added `test_top_10_has_deterministic_tie_break`.

**Rejected:** Reimplementing segmentation in dashboard SQL; Bronze/Silver joins; fabricating Databricks execution evidence.

**Outcome:** **READY FOR COMMIT** (dashboard assets only; Databricks runtime validation remains evaluator step).

