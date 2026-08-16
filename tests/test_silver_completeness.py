"""Tests for Silver completeness validation."""

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

from silver.silver_common import (  # noqa: E402
    DQ_STATUS_FAIL,
    DQ_STATUS_PASS,
    NULL_CUSTOMER_ID,
    NULL_EMAIL,
    NULL_ORDER_ID,
    NULL_PRODUCT_ID,
)
from silver.silver_foundation import apply_silver_foundation  # noqa: E402
from data_generation.generate_sample_data import (  # noqa: E402
    NULL_EMAIL_COUNT,
    NULL_ORDER_CUSTOMER_ID_COUNT,
    NULL_ORDER_PRODUCT_ID_COUNT,
    NUM_CUSTOMERS,
    NUM_ORDERS,
    NUM_PRODUCTS,
    generate_all,
)
from bronze.bronze_common import BronzeConfig, CUSTOMERS_CSV_SCHEMA, ORDERS_CSV_SCHEMA, prepare_bronze_dataframe  # noqa: E402
from silver_test_fixtures import SAMPLE_CUSTOMER_ROW  # noqa: E402

completeness = importlib.import_module("silver.01_quality_completeness")

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


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


def test_null_email_fails_completeness(spark: SparkSession):
    row = (
        1,
        "Alice",
        None,
        "US",
        date(2020, 1, 1),
        "Basic",
        Decimal("100.00"),
    )
    df = spark.createDataFrame([row], schema=CUSTOMERS_CSV_SCHEMA)
    result = completeness.validate_customers_completeness(df).collect()[0]
    assert NULL_EMAIL in result.dq_failure_reasons
    assert result.dq_status == DQ_STATUS_FAIL


def test_blank_email_fails_completeness(spark: SparkSession):
    row = (
        1,
        "Alice",
        "   ",
        "US",
        date(2020, 1, 1),
        "Basic",
        Decimal("100.00"),
    )
    df = spark.createDataFrame([row], schema=CUSTOMERS_CSV_SCHEMA)
    result = completeness.validate_customers_completeness(df).collect()[0]
    assert NULL_EMAIL in result.dq_failure_reasons


def test_valid_email_passes_completeness(spark: SparkSession):
    df = spark.createDataFrame([SAMPLE_CUSTOMER_ROW], schema=CUSTOMERS_CSV_SCHEMA)
    result = completeness.validate_customers_completeness(df).collect()[0]
    assert NULL_EMAIL not in (result.dq_failure_reasons or [])
    assert result.dq_status == DQ_STATUS_PASS


def test_null_order_customer_id_fails(spark: SparkSession):
    row = (
        1,
        None,
        date(2020, 1, 1),
        "10",
        1,
        Decimal("9.99"),
        Decimal("9.99"),
        "Completed",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    result = completeness.validate_orders_completeness(df).collect()[0]
    assert NULL_CUSTOMER_ID in result.dq_failure_reasons


def test_null_order_product_id_fails(spark: SparkSession):
    row = (
        1,
        "100",
        date(2020, 1, 1),
        None,
        1,
        Decimal("9.99"),
        Decimal("9.99"),
        "Completed",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    result = completeness.validate_orders_completeness(df).collect()[0]
    assert NULL_PRODUCT_ID in result.dq_failure_reasons


def test_multiple_completeness_reasons(spark: SparkSession):
    row = (
        1,
        None,
        date(2020, 1, 1),
        None,
        1,
        Decimal("9.99"),
        Decimal("9.99"),
        "Completed",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    result = completeness.validate_orders_completeness(df).collect()[0]
    assert NULL_CUSTOMER_ID in result.dq_failure_reasons
    assert NULL_PRODUCT_ID in result.dq_failure_reasons
    assert len(result.dq_failure_reasons) >= 2


def test_failed_rows_remain_in_dataframe(spark: SparkSession):
    rows = [
        (
            1,
            "Alice",
            None,
            "US",
            date(2020, 1, 1),
            "Basic",
            Decimal("100.00"),
        ),
        SAMPLE_CUSTOMER_ROW,
    ]
    df = spark.createDataFrame(rows, schema=CUSTOMERS_CSV_SCHEMA)
    result = completeness.validate_customers_completeness(df)
    assert result.count() == 2


def test_full_dataset_completeness_counts(
    spark: SparkSession, bronze_config: BronzeConfig
):
    batch_id = "batch_silver"
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)

    customers = prepare_bronze_dataframe(spark, "customers", bronze_config, batch_id, ts)
    customers = completeness.validate_customers_completeness(customers)
    assert customers.count() == NUM_CUSTOMERS
    from pyspark.sql import functions as F

    null_email = customers.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_EMAIL)).count()
    assert null_email == NULL_EMAIL_COUNT

    orders = prepare_bronze_dataframe(spark, "orders", bronze_config, batch_id, ts)
    orders = completeness.validate_orders_completeness(orders)
    assert orders.count() == NUM_ORDERS
    null_cust = orders.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_CUSTOMER_ID)).count()
    null_prod = orders.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_PRODUCT_ID)).count()
    assert null_cust == NULL_ORDER_CUSTOMER_ID_COUNT
    assert null_prod == NULL_ORDER_PRODUCT_ID_COUNT


def test_foundation_preserves_row_counts(
    spark: SparkSession, bronze_config: BronzeConfig
):
    batch_id = "batch_silver"
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)
    for entity, expected in (
        ("customers", NUM_CUSTOMERS),
        ("products", NUM_PRODUCTS),
        ("orders", NUM_ORDERS),
    ):
        bronze = prepare_bronze_dataframe(spark, entity, bronze_config, batch_id, ts)
        silver = apply_silver_foundation(entity, bronze)
        assert silver.count() == expected


def test_full_foundation_dq_counts_on_generated_data(
    spark: SparkSession, bronze_config: BronzeConfig
):
    """Validate Silver completeness/type counts (without RI — orders RI needs references)."""
    from pyspark.sql import functions as F

    batch_id = "batch_silver_validation"
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)

    customers_bronze = prepare_bronze_dataframe(
        spark, "customers", bronze_config, batch_id, ts
    )
    customers = apply_silver_foundation("customers", customers_bronze)
    null_email = customers.filter(
        F.array_contains(F.col("dq_failure_reasons"), NULL_EMAIL)
    ).count()
    assert null_email == NULL_EMAIL_COUNT

    orders_bronze = prepare_bronze_dataframe(spark, "orders", bronze_config, batch_id, ts)
    orders = apply_silver_foundation("orders", orders_bronze)
    null_cust = orders.filter(
        F.array_contains(F.col("dq_failure_reasons"), NULL_CUSTOMER_ID)
    ).count()
    null_prod = orders.filter(
        F.array_contains(F.col("dq_failure_reasons"), NULL_PRODUCT_ID)
    ).count()
    assert null_cust == NULL_ORDER_CUSTOMER_ID_COUNT
    assert null_prod == NULL_ORDER_PRODUCT_ID_COUNT

    from silver.silver_common import INVALID_CUSTOMER_ID_TYPE, INVALID_PRODUCT_ID_TYPE

    assert orders.filter(
        F.array_contains(F.col("dq_failure_reasons"), INVALID_CUSTOMER_ID_TYPE)
    ).count() == 0
    assert orders.filter(
        F.array_contains(F.col("dq_failure_reasons"), INVALID_PRODUCT_ID_TYPE)
    ).count() == 0
