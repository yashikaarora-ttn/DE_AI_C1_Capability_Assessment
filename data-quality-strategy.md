# Data Quality Strategy

How data quality is validated, reported, and consumed across the Medallion pipeline.

---

## Principles

1. **Detect, flag, retain** — Bad records are never silently deleted.
2. **Separate failure types** — Completeness, uniqueness, referential integrity, and business rules use distinct reason codes.
3. **Measure everything** — Each validation run produces counts and pass percentages.
4. **Gold trusts Silver** — Only `dq_status = 'PASS'` rows feed business aggregations (planned).
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
| orders | `quantity > 0` | BR-ORD-01 | `INVALID_QUANTITY` |
| orders | `unit_price >= 0` | BR-ORD-03 | `INVALID_UNIT_PRICE` |
| orders | `total_amount ≈ quantity × unit_price` (±0.01) | BR-ORD-04 | `INVALID_TOTAL_AMOUNT` |
| orders | `order_status` in allowed set | BR-ORD-05 | `INVALID_ORDER_STATUS` |
| orders | `payment_date >= order_date` when both set | BR-ORD-06 | `INVALID_PAYMENT_DATE` |
| orders | `customer_id` / `product_id` parseable integer | TYPE-ORD-01/02 | `INVALID_CUSTOMER_ID_TYPE` / `INVALID_PRODUCT_ID_TYPE` |
| customers | `customer_segment` in allowed set | BR-CUST-02 | `INVALID_CUSTOMER_SEGMENT` |
| customers | `lifetime_value >= 0` | BR-CUST-03 | `INVALID_LIFETIME_VALUE` |
| products | `cost >= 0`, `stock_quantity >= 0`, `reorder_level >= 0` | BR-PROD-02–04 | `INVALID_COST`, etc. |

Bronze order FKs are STRING; Silver normalizes valid integer-like values (`8952`, `8952.0`) to INTEGER. Malformed non-null values (e.g. `ABC`, `12.5`, blank) receive type reason codes — not silent NULL without a reason.

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

**Verification (Phase 3):** Full Silver validation on generated data (seed 42) confirms all intentional issue counts. Customers: 50 `NULL_EMAIL`, 10 `DUPLICATE_CUSTOMER_ID`. Orders: 100 `NULL_CUSTOMER_ID`, 200 `NULL_PRODUCT_ID`, 50 `INVALID_CUSTOMER_ID`, 30 `INVALID_PRODUCT_ID`, 20 `DUPLICATE_ORDER_ID`. Products: 0 duplicate failures.

---

## Row-Level Flags (Silver — implemented foundation)

Each Silver row includes:

```text
dq_status           STRING    -- PASS | FAIL
dq_failure_reasons  ARRAY<STRING>  -- stable reason codes (empty array when PASS)
_silver_processed_at TIMESTAMP
```

**Evaluation:** All applicable rules run independently; failures accumulate in `dq_failure_reasons`.

**PASS/FAIL derivation:** `FAIL` when `size(dq_failure_reasons) > 0`; otherwise `PASS`.

**Blank vs NULL:** For required string fields (e.g. `email`, Bronze STRING FKs), blank/whitespace-only values are treated as incomplete (same reason codes as NULL).

**Example:**

| customer_id | email | dq_status | dq_failure_reasons |
|-------------|-------|-----------|-------------------|
| 42 | NULL | FAIL | `["NULL_EMAIL"]` |
| 100 | alice@example.com | FAIL | `["DUPLICATE_CUSTOMER_ID"]` |

**FK failure separation:** NULL/blank FKs → completeness codes only. Malformed non-null FK strings → `INVALID_*_ID_TYPE` only. Non-null normalized FKs missing from parent → `INVALID_CUSTOMER_ID` / `INVALID_PRODUCT_ID` (RI). A row may accumulate multiple codes across stages; `array_distinct` prevents duplicates within the array.

---

## Implementation Status (Phase 3)

| Module | Status |
|--------|--------|
| `01_quality_completeness.py` | **Implemented** |
| `02_quality_uniqueness.py` | **Implemented** (window-based PK duplicate detection) |
| `03_quality_type_validation.py` | **Implemented** (FK normalization + business/type foundation) |
| `04_quality_referential_integrity.py` | **Implemented** (broadcast join against parent keys) |
| `silver_common.py` | **Implemented** (shared helpers, reason codes, `append_validation_reasons`) |
| `silver_foundation.py` | **Implemented** (orchestrates completeness → type → uniqueness → RI) |
| `05_quality_business_logic.py` | Not yet (extra rules may merge here later) |
| `create_silver_tables.py` | **Implemented** |
| `dq_metrics.py` | **Implemented** |
| `05_quality_business_logic.py` | **Implemented** (delegates to type validation) |
| DQ metrics Delta table | **Implemented** (`silver_dq_metrics`, append) |

### Validation order

1. Completeness (`01_quality_completeness.py`)
2. Type / business rules (`03_quality_type_validation.py`) — includes order FK STRING→INTEGER normalization
3. Uniqueness (`02_quality_uniqueness.py`)
4. Referential integrity (`04_quality_referential_integrity.py`) — orders only; requires processed customer/product Silver DataFrames

### Reason codes (foundation)

| Code | Category | Meaning |
|------|----------|---------|
| `NULL_EMAIL` | Completeness | Missing or blank email |
| `NULL_CUSTOMER_ID` | Completeness | Missing or blank order `customer_id` (Bronze STRING) |
| `NULL_PRODUCT_ID` | Completeness | Missing or blank order `product_id` |
| `NULL_ORDER_ID` | Completeness | Missing `order_id` |
| `NULL_PRODUCT_ID` (products) | Completeness | Missing `product_id` on products |
| `INVALID_CUSTOMER_ID_TYPE` | Type | Non-null FK string not parseable as integer |
| `INVALID_PRODUCT_ID_TYPE` | Type | Non-null FK string not parseable as integer |
| `INVALID_INTEGER_RANGE` | Type | Parsed integer outside Spark INT range |
| `INVALID_QUANTITY` | Business | NULL or `quantity <= 0` |
| `INVALID_UNIT_PRICE` | Business | NULL or negative `unit_price` |
| `INVALID_TOTAL_AMOUNT` | Business | `total_amount` not ≈ `quantity × unit_price` |
| `INVALID_CUSTOMER_SEGMENT` | Business | Segment not in allowed set |
| `INVALID_ORDER_STATUS` | Business | Status not in allowed set |
| `INVALID_PRICE` / `INVALID_COST` | Business | Product price/cost rules |
| `INVALID_PAYMENT_DATE` | Business | `payment_date` before `order_date` |
| `DUPLICATE_CUSTOMER_ID` | Uniqueness | Non-unique `customer_id` (all rows in duplicate group flagged) |
| `DUPLICATE_ORDER_ID` | Uniqueness | Non-unique `order_id` (all rows in duplicate group flagged) |
| `DUPLICATE_PRODUCT_ID` | Uniqueness | Non-unique `product_id` |
| `INVALID_CUSTOMER_ID` | Referential integrity | Normalized order `customer_id` not found in customers |
| `INVALID_PRODUCT_ID` | Referential integrity | Normalized order `product_id` not found in products |

### FK failure taxonomy (orders)

| Situation | Reason code(s) | Notes |
|-----------|----------------|-------|
| NULL or blank Bronze STRING FK | `NULL_CUSTOMER_ID` / `NULL_PRODUCT_ID` | Completeness only — no RI code |
| Malformed non-null FK (`ABC`, `12.5`) | `INVALID_CUSTOMER_ID_TYPE` / `INVALID_PRODUCT_ID_TYPE` | Type validation only — no RI code |
| Valid integer FK not in parent table | `INVALID_CUSTOMER_ID` / `INVALID_PRODUCT_ID` | RI only when FK normalized successfully |

Reason codes accumulate across stages via `array_distinct(array_union(...))`. `dq_status = FAIL` when `size(dq_failure_reasons) > 0`.

---

## Row-Level Flags (legacy reference — superseded)

Previous docs used `is_valid` BOOLEAN and delimited STRING reasons. Current implementation uses `dq_status` and `ARRAY<STRING>` for easier multi-reason querying in Spark.

---

## DQ Metrics (`silver_dq_metrics`)

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
| `valid_records` | `dq_status = 'PASS'` count |
| `invalid_records` | `dq_status = 'FAIL'` count |
| `valid_percentage` | Valid row percentage |

Metrics enable assessment reviewers to confirm that intentional DQ issues were detected at expected volumes.

---

## Bad Record Retention

| Layer | Invalid row handling |
|-------|---------------------|
| Bronze | All rows retained (raw) |
| Silver | All rows retained with `dq_status` and `dq_failure_reasons` |
| Gold | Invalid rows **excluded** from aggregations via `WHERE dq_status = 'PASS'` (planned) |
| Dashboard | Reads Gold only; no exposure of invalid Silver rows |

Invalid rows remain queryable:

```sql
-- Example (planned): audit invalid orders
SELECT order_id, customer_id, product_id, dq_failure_reasons
FROM silver_orders
WHERE dq_status = 'FAIL';
```

---

## Reporting (Planned)

- Silver validation notebook section displaying latest `silver_dq_metrics`
- Optional SQL view: `vw_dq_summary_latest`
- Debugging notes when observed metrics diverge from expected counts

Reporting implementation: metrics built in-process and written to `silver_dq_metrics` (append). Local tests validate metrics accuracy; Databricks Delta persistence prepared but not integration-tested in this repo.
