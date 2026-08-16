# Data Quality Strategy

How data quality is validated, reported, and consumed across the Medallion pipeline.

---

## Principles

1. **Detect, flag, retain** — Bad records are never silently deleted.
2. **Separate failure types** — Completeness, uniqueness, referential integrity, and business rules use distinct reason codes.
3. **Measure everything** — Each validation run produces counts and pass percentages.
4. **Gold trusts Silver** — Only `is_valid = true` rows feed business aggregations.
5. **Auditable** — Reason codes and metrics support debugging and assessment review.

---

## Validation Categories

### Completeness

Required fields must not be NULL.

| Entity | Column | Rule ID | Reason code |
|--------|--------|---------|-------------|
| customers | `customer_id` | COMP-CUST-01 | `NULL_CUSTOMER_ID` |
| customers | `email` | COMP-CUST-02 | `NULL_EMAIL` |
| products | `product_id` | COMP-PROD-01 | `NULL_PRODUCT_ID` |
| orders | `order_id` | COMP-ORD-01 | `NULL_ORDER_ID` |
| orders | `customer_id` | COMP-ORD-02 | `NULL_CUSTOMER_ID` |
| orders | `product_id` | COMP-ORD-03 | `NULL_PRODUCT_ID` |

### Uniqueness

Primary key values must be unique within each entity.

| Entity | Column | Rule ID | Reason code |
|--------|--------|---------|-------------|
| customers | `customer_id` | UNIQ-CUST-01 | `DUPLICATE_CUSTOMER_ID` |
| products | `product_id` | UNIQ-PROD-01 | `DUPLICATE_PRODUCT_ID` |
| orders | `order_id` | UNIQ-ORD-01 | `DUPLICATE_ORDER_ID` |

**Duplicate handling:** All rows sharing a duplicated key are flagged (not just the "second" occurrence).

### Referential Integrity

Foreign keys must reference existing parent keys.

| Child | FK column | Parent | Rule ID | Reason code |
|-------|-----------|--------|---------|-------------|
| orders | `customer_id` | customers.customer_id | RI-ORD-01 | `INVALID_CUSTOMER_ID` |
| orders | `product_id` | products.product_id | RI-ORD-02 | `INVALID_PRODUCT_ID` |

**Note:** NULL FKs are completeness failures (`NULL_CUSTOMER_ID`, `NULL_PRODUCT_ID`), not referential integrity failures. RI rules apply only when the FK value is non-NULL but does not exist in the parent table.

### Type & Business-Rule Validation

| Entity | Rule | Rule ID | Reason code |
|--------|------|---------|-------------|
| products | `price > 0` | BR-PROD-01 | `INVALID_PRICE` |
| orders | `quantity >= 1` | BR-ORD-01 | `INVALID_QUANTITY` |
| customers | `signup_date` parseable as DATE | BR-CUST-01 | `INVALID_SIGNUP_DATE` |
| orders | `order_date` parseable as DATE | BR-ORD-02 | `INVALID_ORDER_DATE` |

Type coercion issues discovered in Bronze may be flagged here if values cannot be cast safely.

---

## Intentional Data-Quality Issues (Assessment Requirements)

These issues will be **injected during data generation** and must be **detected in Silver**:

| Dataset | Issue | Expected count | Mapped rules |
|---------|-------|----------------|--------------|
| customers | NULL `email` | 50 | COMP-CUST-02 |
| customers | Duplicate `customer_id` | 10 | UNIQ-CUST-01 |
| orders | NULL `customer_id` | 100 | COMP-ORD-02 |
| orders | NULL `product_id` | 200 | COMP-ORD-03 |
| orders | Invalid `customer_id` | 50 | RI-ORD-01 |
| orders | Invalid `product_id` | 30 | RI-ORD-02 |
| orders | Duplicate `order_id` | 20 | UNIQ-ORD-01 |

**Verification:** Data generation tests will assert these exact counts. Silver DQ metrics should reflect them after a full pipeline run (to be validated in later phases).

---

## Row-Level Flags

Each Silver row includes:

```text
is_valid          BOOLEAN   -- false if ANY rule fails
dq_failure_reasons STRING   -- e.g., "NULL_EMAIL|DUPLICATE_CUSTOMER_ID"
```

**Evaluation order:** All applicable rules run independently; all failures are collected.

**Example:**

| customer_id | email | is_valid | dq_failure_reasons |
|-------------|-------|----------|-------------------|
| 42 | NULL | false | `NULL_EMAIL` |
| 100 | alice@example.com | false | `DUPLICATE_CUSTOMER_ID` |
| 100 | alice2@example.com | false | `DUPLICATE_CUSTOMER_ID` |

---

## DQ Metrics & Pass Percentages

A metrics table (planned: `silver_dq_metrics`) will be written after each Silver validation run:

| Column | Description |
|--------|-------------|
| `run_id` | Pipeline run identifier |
| `run_timestamp` | When validation completed |
| `entity` | customers / orders / products |
| `rule_id` | Rule identifier (e.g., COMP-CUST-02) |
| `reason_code` | Failure reason code |
| `total_records` | Rows evaluated |
| `passed_count` | Rows passing this rule |
| `failed_count` | Rows failing this rule |
| `pass_percentage` | `passed_count / total_records × 100` |

**Table-level summary (planned):**

| Column | Description |
|--------|-------------|
| `entity` | Entity name |
| `total_records` | Row count |
| `valid_records` | `is_valid = true` count |
| `invalid_records` | `is_valid = false` count |
| `valid_percentage` | Valid row percentage |

Metrics enable assessment reviewers to confirm that intentional DQ issues were detected at expected volumes.

---

## Bad Record Retention

| Layer | Invalid row handling |
|-------|---------------------|
| Bronze | All rows retained (raw) |
| Silver | All rows retained with `is_valid` flag and reason codes |
| Gold | Invalid rows **excluded** from aggregations via `WHERE is_valid = true` |
| Dashboard | Reads Gold only; no exposure of invalid Silver rows |

Invalid rows remain queryable:

```sql
-- Example (planned): audit invalid orders
SELECT order_id, customer_id, product_id, dq_failure_reasons
FROM silver_orders
WHERE is_valid = false;
```

---

## Reporting (Planned)

- Silver validation notebook section displaying latest `silver_dq_metrics`
- Optional SQL view: `vw_dq_summary_latest`
- Debugging notes when observed metrics diverge from expected counts

Reporting implementation is planned for the Silver phase. No execution results are documented yet.
