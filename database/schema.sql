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
-- Silver: customers (validated + DQ flags)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.silver_customers (
    customer_id           INT             NOT NULL,
    customer_name         STRING          NOT NULL,
    email                 STRING,
    country               STRING          NOT NULL,
    signup_date           DATE            NOT NULL,
    customer_segment      STRING          NOT NULL,
    lifetime_value        DECIMAL(12, 2)  NOT NULL,
    _ingestion_timestamp  TIMESTAMP       NOT NULL,
    _source_file          STRING          NOT NULL,
    _batch_id             STRING          NOT NULL,
    dq_status             STRING          NOT NULL,
    dq_failure_reasons    ARRAY<STRING>,
    _silver_processed_at  TIMESTAMP       NOT NULL
)
USING DELTA
COMMENT 'Silver validated customers — all rows retained with DQ flags'
LOCATION '${STORAGE_PATH}/silver/silver_customers';

-- -----------------------------------------------------------------------------
-- Silver: products
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.silver_products (
    product_id            INT             NOT NULL,
    product_name          STRING          NOT NULL,
    category              STRING          NOT NULL,
    price                 DECIMAL(10, 2)  NOT NULL,
    cost                  DECIMAL(10, 2)  NOT NULL,
    stock_quantity        INT             NOT NULL,
    reorder_level         INT             NOT NULL,
    _ingestion_timestamp  TIMESTAMP       NOT NULL,
    _source_file          STRING          NOT NULL,
    _batch_id             STRING          NOT NULL,
    dq_status             STRING          NOT NULL,
    dq_failure_reasons    ARRAY<STRING>,
    _silver_processed_at  TIMESTAMP       NOT NULL
)
USING DELTA
LOCATION '${STORAGE_PATH}/silver/silver_products';

-- -----------------------------------------------------------------------------
-- Silver: orders (normalized INTEGER FKs)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.silver_orders (
    order_id              INT             NOT NULL,
    customer_id           INT,
    order_date            DATE            NOT NULL,
    product_id            INT,
    quantity              INT             NOT NULL,
    unit_price            DECIMAL(10, 2)  NOT NULL,
    total_amount          DECIMAL(12, 2)  NOT NULL,
    order_status          STRING          NOT NULL,
    payment_date          DATE,
    _ingestion_timestamp  TIMESTAMP       NOT NULL,
    _source_file          STRING          NOT NULL,
    _batch_id             STRING          NOT NULL,
    dq_status             STRING          NOT NULL,
    dq_failure_reasons    ARRAY<STRING>,
    _silver_processed_at  TIMESTAMP       NOT NULL
)
USING DELTA
LOCATION '${STORAGE_PATH}/silver/silver_orders';

-- -----------------------------------------------------------------------------
-- Silver: DQ metrics (append per run)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.silver_dq_metrics (
    entity_name           STRING          NOT NULL,
    metric_type           STRING          NOT NULL,
    reason_code           STRING,
    rule_id               STRING,
    total_records         INT             NOT NULL,
    failed_count          INT             NOT NULL,
    passed_count          INT             NOT NULL,
    failed_percentage     DOUBLE          NOT NULL,
    passed_percentage     DOUBLE          NOT NULL,
    batch_id              STRING          NOT NULL,
    metric_timestamp      TIMESTAMP       NOT NULL
)
USING DELTA
COMMENT 'Silver DQ metrics — RULE per reason code + OVERALL row-level per entity'
LOCATION '${STORAGE_PATH}/silver/silver_dq_metrics';

-- -----------------------------------------------------------------------------
-- Gold: sales by product
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.gold_sales_by_product (
    product_id            INT             NOT NULL,
    product_name          STRING          NOT NULL,
    category              STRING          NOT NULL,
    total_orders          BIGINT          NOT NULL,
    total_revenue         DECIMAL(32, 2)  NOT NULL,
    avg_order_value       DECIMAL(32, 6)  NOT NULL
)
USING DELTA
COMMENT 'Gold sales by product — trusted business orders only'
LOCATION '${STORAGE_PATH}/gold/gold_sales_by_product';

-- -----------------------------------------------------------------------------
-- Gold: revenue by customer
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.gold_revenue_by_customer (
    customer_id           INT             NOT NULL,
    customer_name         STRING          NOT NULL,
    customer_segment      STRING          NOT NULL,
    total_orders          BIGINT          NOT NULL,
    total_revenue         DECIMAL(32, 2)  NOT NULL,
    avg_order_value       DECIMAL(32, 6)  NOT NULL,
    lifetime_value_actual DECIMAL(32, 2)  NOT NULL
)
USING DELTA
COMMENT 'Gold revenue by customer — all PASS customers; observed lifetime_value_actual'
LOCATION '${STORAGE_PATH}/gold/gold_revenue_by_customer';

-- -----------------------------------------------------------------------------
-- Gold: daily / weekly trends
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.gold_daily_weekly_trends (
    period_type           STRING          NOT NULL,
    period_start          DATE            NOT NULL,
    total_orders          BIGINT          NOT NULL,
    total_revenue         DECIMAL(32, 2)  NOT NULL,
    avg_order_value       DECIMAL(32, 6)  NOT NULL
)
USING DELTA
COMMENT 'Gold daily and weekly trends from trusted business orders'
LOCATION '${STORAGE_PATH}/gold/gold_daily_weekly_trends';

-- -----------------------------------------------------------------------------
-- Gold: customer segmentation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}.gold_customer_segmentation (
    segment_type          STRING          NOT NULL,
    customer_count        BIGINT          NOT NULL,
    avg_revenue           DECIMAL(32, 6)  NOT NULL,
    total_revenue         DECIMAL(32, 2)  NOT NULL
)
USING DELTA
COMMENT 'Gold customer segmentation — mutually exclusive segments over PASS customers'
LOCATION '${STORAGE_PATH}/gold/gold_customer_segmentation';

