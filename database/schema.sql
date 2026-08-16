-- =============================================================================
-- Databricks Medallion Pipeline — Schema Reference
-- =============================================================================
-- Environment placeholders (replace before execution):
--   ${CATALOG_NAME}  e.g., main
--   ${SCHEMA_NAME}   e.g., ecommerce_medallion
--   ${STORAGE_PATH}  e.g., abfss://container@account.dfs.core.windows.net/medallion
--
-- Bronze tables are created by PySpark Delta writes (src/bronze/).
-- DDL below documents the logical schema for Unity Catalog.
-- =============================================================================

-- CREATE CATALOG IF NOT EXISTS ${CATALOG_NAME};
-- CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME};

-- -----------------------------------------------------------------------------
-- Bronze: customers (matches Phase 1 CSV + metadata)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.bronze_customers (
    customer_id           INT             NOT NULL,
    customer_name         STRING          NOT NULL,
    email                 STRING,
    country               STRING          NOT NULL,
    signup_date           DATE            NOT NULL,
    customer_segment      STRING          NOT NULL,
    lifetime_value        DECIMAL(12, 2)  NOT NULL,
    _ingestion_timestamp  TIMESTAMP       NOT NULL,
    _source_file          STRING          NOT NULL,
    _batch_id             STRING          NOT NULL
)
USING DELTA
COMMENT 'Bronze raw customers — preserves source DQ issues'
LOCATION '${STORAGE_PATH}/bronze/bronze_customers';

-- -----------------------------------------------------------------------------
-- Bronze: products
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.bronze_products (
    product_id            INT             NOT NULL,
    product_name          STRING          NOT NULL,
    category              STRING          NOT NULL,
    price                 DECIMAL(10, 2)  NOT NULL,
    cost                  DECIMAL(10, 2)  NOT NULL,
    stock_quantity        INT             NOT NULL,
    reorder_level         INT             NOT NULL,
    _ingestion_timestamp  TIMESTAMP       NOT NULL,
    _source_file          STRING          NOT NULL,
    _batch_id             STRING          NOT NULL
)
USING DELTA
LOCATION '${STORAGE_PATH}/bronze/bronze_products';

-- -----------------------------------------------------------------------------
-- Bronze: orders
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.bronze_orders (
    order_id              INT             NOT NULL,
    customer_id           STRING,
    order_date            DATE            NOT NULL,
    product_id            STRING,
    quantity              INT             NOT NULL,
    unit_price            DECIMAL(10, 2)  NOT NULL,
    total_amount          DECIMAL(12, 2)  NOT NULL,
    order_status          STRING          NOT NULL,
    payment_date          DATE,
    _ingestion_timestamp  TIMESTAMP       NOT NULL,
    _source_file          STRING          NOT NULL,
    _batch_id             STRING          NOT NULL
)
USING DELTA
LOCATION '${STORAGE_PATH}/bronze/bronze_orders';

-- -----------------------------------------------------------------------------
-- Bronze: ingestion log (append per run)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.bronze_ingestion_log (
    entity_name           STRING          NOT NULL,
    source_file           STRING          NOT NULL,
    row_count             INT             NOT NULL,
    ingestion_timestamp   TIMESTAMP       NOT NULL,
    batch_id              STRING          NOT NULL,
    status                STRING          NOT NULL
)
USING DELTA
LOCATION '${STORAGE_PATH}/bronze/bronze_ingestion_log';

-- -----------------------------------------------------------------------------
-- Silver / Gold — implemented in later phases
-- -----------------------------------------------------------------------------
