"""Tests for Silver referential integrity validation."""

from __future__ import annotations

import importlib
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from bronze.bronze_common import (  # noqa: E402
    CUSTOMERS_CSV_SCHEMA,
    ORDERS_CSV_SCHEMA,
    PRODUCTS_CSV_SCHEMA,
    BronzeConfig,
    prepare_bronze_dataframe,
)
from data_generation.generate_sample_data import (  # noqa: E402
    DUPLICATE_CUSTOMER_ID_ROWS,
    DUPLICATE_ORDER_ID_ROWS,
    NULL_EMAIL_COUNT,
    NULL_ORDER_CUSTOMER_ID_COUNT,
    NULL_ORDER_PRODUCT_ID_COUNT,
    NUM_CUSTOMERS,
    NUM_ORDERS,
    NUM_PRODUCTS,
    generate_all,
)
from silver.silver_common import (  # noqa: E402
    DQ_STATUS_FAIL,
    DQ_STATUS_PASS,
    DUPLICATE_CUSTOMER_ID,
    DUPLICATE_ORDER_ID,
    DUPLICATE_PRODUCT_ID,
    INVALID_CUSTOMER_ID,
    INVALID_CUSTOMER_ID_TYPE,
    INVALID_PRODUCT_ID,
    INVALID_PRODUCT_ID_TYPE,
    NULL_CUSTOMER_ID,
    NULL_EMAIL,
    NULL_PRODUCT_ID,
)
from silver.silver_foundation import apply_silver_pipeline, apply_silver_validation  # noqa: E402

ri = importlib.import_module("silver.04_quality_referential_integrity")
type_validation = importlib.import_module("silver.03_quality_type_validation")
completeness = importlib.import_module("silver.01_quality_completeness")

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


@pytest.fixture
def sample_data_dir(tmp_path) -> Path:
    generate_all(seed=42, output_dir=str(tmp_path))
    return tmp_path


@pytest.fixture
def bronze_config(sample_data_dir: Path) -> BronzeConfig:
    return BronzeConfig(
        catalog=None,
        schema_name="silver_test",
        storage_path=None,
        input_dir=sample_data_dir,
    )


def _minimal_customers(spark: SparkSession):
    rows = [
        (1, "Alice", "a@b.com", "US", date(2020, 1, 1), "Basic", Decimal("100.00")),
    ]
    return spark.createDataFrame(rows, schema=CUSTOMERS_CSV_SCHEMA)


def _minimal_products(spark: SparkSession):
    rows = [(10, "Prod", "Cat", Decimal("10.00"), Decimal("5.00"), 100, 10)]
    return spark.createDataFrame(rows, schema=PRODUCTS_CSV_SCHEMA)


def test_valid_customer_reference_passes_ri(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "1",
        date(2020, 1, 1),
        "10",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        date(2020, 1, 2),
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert INVALID_CUSTOMER_ID not in (row.dq_failure_reasons or [])


def test_invalid_customer_reference_flagged(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "999",
        date(2020, 1, 1),
        "10",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        date(2020, 1, 2),
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert INVALID_CUSTOMER_ID in row.dq_failure_reasons


def test_null_customer_fk_no_ri_failure(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        None,
        date(2020, 1, 1),
        "10",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        None,
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert NULL_CUSTOMER_ID in row.dq_failure_reasons
    assert INVALID_CUSTOMER_ID not in row.dq_failure_reasons


def test_malformed_fk_no_ri_failure(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "ABC",
        date(2020, 1, 1),
        "10",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        None,
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert INVALID_CUSTOMER_ID_TYPE in row.dq_failure_reasons
    assert INVALID_CUSTOMER_ID not in row.dq_failure_reasons


def test_invalid_product_reference_flagged(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "1",
        date(2020, 1, 1),
        "999",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        date(2020, 1, 2),
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert INVALID_PRODUCT_ID in row.dq_failure_reasons


def test_null_product_fk_no_ri_failure(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "1",
        date(2020, 1, 1),
        None,
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        None,
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert NULL_PRODUCT_ID in row.dq_failure_reasons
    assert INVALID_PRODUCT_ID not in row.dq_failure_reasons


def test_ri_preserves_previous_reasons(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "999",
        date(2020, 1, 1),
        "999",
        0,
        Decimal("10.00"),
        Decimal("25.00"),
        "Shipped",
        None,
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = completeness.validate_orders_completeness(orders)
    orders = type_validation.validate_orders_type_rules(orders)
    orders = type_validation.drop_order_fk_raw_columns(orders)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    row = orders.collect()[0]
    assert INVALID_CUSTOMER_ID in row.dq_failure_reasons
    assert INVALID_PRODUCT_ID in row.dq_failure_reasons
    assert len(row.dq_failure_reasons) >= 2


def test_ri_does_not_delete_rows(spark: SparkSession):
    customers = _minimal_customers(spark)
    products = _minimal_products(spark)
    order_row = (
        1,
        "999",
        date(2020, 1, 1),
        "10",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        None,
    )
    orders = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    orders = ri.validate_orders_referential_integrity(orders, customers, products)
    assert orders.count() == 1


def test_full_pipeline_dq_counts_and_pass_fail(
    spark: SparkSession, bronze_config: BronzeConfig
):
    batch_id = "full_silver"
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)

    bronze_customers = prepare_bronze_dataframe(
        spark, "customers", bronze_config, batch_id, ts
    )
    bronze_products = prepare_bronze_dataframe(
        spark, "products", bronze_config, batch_id, ts
    )
    bronze_orders = prepare_bronze_dataframe(spark, "orders", bronze_config, batch_id, ts)

    customers, products, orders = apply_silver_pipeline(
        bronze_customers, bronze_products, bronze_orders
    )

    assert customers.count() == NUM_CUSTOMERS
    assert products.count() == NUM_PRODUCTS
    assert orders.count() == NUM_ORDERS

    assert customers.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_EMAIL)).count() == NULL_EMAIL_COUNT
    assert customers.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_CUSTOMER_ID)
    ).count() == DUPLICATE_CUSTOMER_ID_ROWS
    assert products.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_PRODUCT_ID)
    ).count() == 0

    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_CUSTOMER_ID)).count() == NULL_ORDER_CUSTOMER_ID_COUNT
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_PRODUCT_ID)).count() == NULL_ORDER_PRODUCT_ID_COUNT
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), INVALID_CUSTOMER_ID)).count() == 50
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), INVALID_PRODUCT_ID)).count() == 30
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_ORDER_ID)).count() == DUPLICATE_ORDER_ID_ROWS

    customer_fail = customers.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()
    customer_pass = customers.filter(F.col("dq_status") == DQ_STATUS_PASS).count()
    product_fail = products.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()
    product_pass = products.filter(F.col("dq_status") == DQ_STATUS_PASS).count()
    order_fail = orders.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()
    order_pass = orders.filter(F.col("dq_status") == DQ_STATUS_PASS).count()

    assert customer_fail == NULL_EMAIL_COUNT + DUPLICATE_CUSTOMER_ID_ROWS
    assert customer_pass == NUM_CUSTOMERS - customer_fail
    assert product_fail == 0
    assert product_pass == NUM_PRODUCTS
    assert order_fail == (
        NULL_ORDER_CUSTOMER_ID_COUNT
        + NULL_ORDER_PRODUCT_ID_COUNT
        + 50
        + 30
        + DUPLICATE_ORDER_ID_ROWS
    )
    assert order_pass == NUM_ORDERS - order_fail
