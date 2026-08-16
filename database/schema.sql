-- =============================================================================
-- Databricks Medallion Pipeline — Initial Schema Placeholder
-- =============================================================================
-- Purpose: Define source entity structures and Medallion table naming conventions.
-- Status:  Foundation phase — Bronze/Silver/Gold tables are placeholders only.
--
-- Environment placeholders (replace before execution):
--   ${CATALOG_NAME}  e.g., main, dev_catalog
--   ${SCHEMA_NAME}   e.g., ecommerce_medallion
--   ${STORAGE_PATH}  e.g., abfss://container@account.dfs.core.windows.net/medallion
--
-- Note: Actual table creation may use PySpark/Delta writes rather than DDL.
--       Adjust for Unity Catalog vs Hive metastore as needed.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Catalog and schema (Unity Catalog syntax)
-- Uncomment and customize for your workspace.
-- -----------------------------------------------------------------------------

-- CREATE CATALOG IF NOT EXISTS ${CATALOG_NAME};
-- CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}
--   COMMENT 'E-commerce Medallion pipeline — Bronze, Silver, Gold layers';

-- USE ${CATALOG_NAME}.${SCHEMA_NAME};

-- -----------------------------------------------------------------------------
-- Source entity definitions (logical — match CSV and data-model.md)
-- These serve as reference DDL; Bronze ingestion will materialize Delta tables.
-- -----------------------------------------------------------------------------

-- customers: 10,000 rows (source CSV)
-- PK: customer_id
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.source_customers_ref (
    customer_id   INT           NOT NULL COMMENT 'Primary key',
    first_name    STRING        NOT NULL,
    last_name     STRING        NOT NULL,
    email         STRING        COMMENT 'Nullable in source; required in Silver',
    signup_date   DATE          NOT NULL,
    country       STRING        NOT NULL
)
USING DELTA
COMMENT 'Reference DDL for customers source entity — not populated in Phase 0'
LOCATION '${STORAGE_PATH}/reference/source_customers_ref';

-- products: 500 rows (source CSV)
-- PK: product_id
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.source_products_ref (
    product_id    INT             NOT NULL COMMENT 'Primary key',
    product_name  STRING          NOT NULL,
    category      STRING          NOT NULL,
    price         DECIMAL(10, 2)  NOT NULL,
    created_date  DATE            NOT NULL
)
USING DELTA
COMMENT 'Reference DDL for products source entity — not populated in Phase 0'
LOCATION '${STORAGE_PATH}/reference/source_products_ref';

-- orders: 100,000 rows (source CSV)
-- PK: order_id | FK: customer_id, product_id
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.source_orders_ref (
    order_id      INT     NOT NULL COMMENT 'Primary key',
    customer_id   INT     COMMENT 'FK → customers.customer_id',
    product_id    INT     COMMENT 'FK → products.product_id',
    quantity      INT     NOT NULL,
    order_date    DATE    NOT NULL,
    order_status  STRING  NOT NULL
)
USING DELTA
COMMENT 'Reference DDL for orders source entity — not populated in Phase 0'
LOCATION '${STORAGE_PATH}/reference/source_orders_ref';

-- -----------------------------------------------------------------------------
-- Planned Medallion tables (placeholders — implemented in later phases)
-- -----------------------------------------------------------------------------

-- Bronze: bronze_customers, bronze_orders, bronze_products, bronze_ingestion_log
-- Silver: silver_customers, silver_orders, silver_products, silver_dq_metrics
-- Gold:   gold_sales_by_product, gold_revenue_by_customer, gold_customer_segmentation

-- Example Bronze placeholder (structure finalized in Bronze phase):
/*
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.bronze_customers (
    customer_id           INT,
    first_name            STRING,
    last_name             STRING,
    email                 STRING,
    signup_date           STRING,
    country               STRING,
    _ingestion_timestamp  TIMESTAMP,
    _source_file          STRING
)
USING DELTA
LOCATION '${STORAGE_PATH}/bronze/customers';
*/
