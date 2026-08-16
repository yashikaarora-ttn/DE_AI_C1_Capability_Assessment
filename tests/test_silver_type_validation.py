"""Tests for Silver FK normalization and type/business-rule validation."""

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
    DQ_STATUS_PASS,
    INVALID_CUSTOMER_ID_TYPE,
    INVALID_CUSTOMER_SEGMENT,
    INVALID_ORDER_STATUS,
    INVALID_PRODUCT_ID_TYPE,
    INVALID_QUANTITY,
    INVALID_TOTAL_AMOUNT,
    NULL_CUSTOMER_ID,
    normalize_integer_string,
    normalize_order_foreign_keys,
)
from silver.silver_foundation import apply_silver_foundation  # noqa: E402
from data_generation.generate_sample_data import (  # noqa: E402
    INVALID_CUSTOMER_ID_START,
    INVALID_ORDER_CUSTOMER_ID_COUNT,
    INVALID_ORDER_PRODUCT_ID_COUNT,
    INVALID_PRODUCT_ID_START,
    generate_all,
)
from bronze.bronze_common import BronzeConfig, CUSTOMERS_CSV_SCHEMA, ORDERS_CSV_SCHEMA, prepare_bronze_dataframe  # noqa: E402
from silver_test_fixtures import SAMPLE_CUSTOMER_ROW, SAMPLE_ORDER_ROW  # noqa: E402

type_validation = importlib.import_module("silver.03_quality_type_validation")

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def test_normalize_integer_string_plain():
    value, err = normalize_integer_string("8952")
    assert value == 8952
    assert err is None


def test_normalize_integer_string_dot_zero():
    value, err = normalize_integer_string("8952.0")
    assert value == 8952
    assert err is None


def test_normalize_integer_string_whitespace():
    value, err = normalize_integer_string("  8952.0  ")
    assert value == 8952
    assert err is None


def test_normalize_integer_string_null():
    value, err = normalize_integer_string(None)
    assert value is None
    assert err is None


def test_normalize_integer_string_abc_fails():
    value, err = normalize_integer_string("ABC")
    assert value is None
    assert err == INVALID_CUSTOMER_ID_TYPE


def test_normalize_integer_string_decimal_fails():
    value, err = normalize_integer_string("12.5")
    assert value is None
    assert err == INVALID_CUSTOMER_ID_TYPE


def test_normalize_integer_string_blank():
    value, err = normalize_integer_string("")
    assert value is None
    assert err is None


def test_normalize_integer_string_malformed():
    value, err = normalize_integer_string("12.34.56")
    assert value is None
    assert err == INVALID_CUSTOMER_ID_TYPE


def test_spark_fk_normalization_examples(spark: SparkSession):
    df = spark.createDataFrame(
        [
            (1, "8952", "100.0"),
            (2, "8952.0", "200.0"),
            (3, "  42  ", "  7.0  "),
            (4, None, None),
        ],
        ["order_id", "customer_id", "product_id"],
    )
    normalized = normalize_order_foreign_keys(df)
    rows = normalized.select("customer_id", "product_id").collect()
    assert rows[0].customer_id == 8952
    assert rows[0].product_id == 100
    assert rows[1].customer_id == 8952
    assert rows[2].customer_id == 42
    assert rows[2].product_id == 7
    assert rows[3].customer_id is None
    assert rows[3].product_id is None


def test_abc_fk_gets_type_reason_not_null_completeness(spark: SparkSession):
    row = (
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
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    completeness = importlib.import_module("silver.01_quality_completeness")
    df = completeness.validate_orders_completeness(df)
    df = type_validation.validate_orders_type_rules(df)
    row = df.collect()[0]
    assert NULL_CUSTOMER_ID not in row.dq_failure_reasons
    assert INVALID_CUSTOMER_ID_TYPE in row.dq_failure_reasons


def test_invalid_product_fk_type(spark: SparkSession):
    row = (
        1,
        "100",
        date(2020, 1, 1),
        "12.5",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Completed",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    df = type_validation.validate_orders_type_rules(df)
    row = df.collect()[0]
    assert INVALID_PRODUCT_ID_TYPE in row.dq_failure_reasons


def test_invalid_quantity_rule(spark: SparkSession):
    row = (
        1,
        "100",
        date(2020, 1, 1),
        "10",
        0,
        Decimal("10.00"),
        Decimal("0.00"),
        "Completed",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    df = type_validation.validate_orders_type_rules(df)
    row = df.collect()[0]
    assert INVALID_QUANTITY in row.dq_failure_reasons


def test_invalid_total_amount_rule(spark: SparkSession):
    row = (
        1,
        "100",
        date(2020, 1, 1),
        "10",
        2,
        Decimal("10.00"),
        Decimal("25.00"),
        "Completed",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    df = type_validation.validate_orders_type_rules(df)
    row = df.collect()[0]
    assert INVALID_TOTAL_AMOUNT in row.dq_failure_reasons


def test_invalid_customer_segment(spark: SparkSession):
    row = (
        1,
        "Alice",
        "a@b.com",
        "US",
        date(2020, 1, 1),
        "VIP",
        Decimal("100.00"),
    )
    df = spark.createDataFrame([row], schema=CUSTOMERS_CSV_SCHEMA)
    df = type_validation.validate_customers_type_rules(df)
    row = df.collect()[0]
    assert INVALID_CUSTOMER_SEGMENT in row.dq_failure_reasons


def test_invalid_order_status(spark: SparkSession):
    row = (
        1,
        "100",
        date(2020, 1, 1),
        "10",
        1,
        Decimal("10.00"),
        Decimal("10.00"),
        "Shipped",
        None,
    )
    df = spark.createDataFrame([row], schema=ORDERS_CSV_SCHEMA)
    df = type_validation.validate_orders_type_rules(df)
    row = df.collect()[0]
    assert INVALID_ORDER_STATUS in row.dq_failure_reasons


def test_valid_order_passes_type_rules(spark: SparkSession):
    df = spark.createDataFrame([SAMPLE_ORDER_ROW], schema=ORDERS_CSV_SCHEMA)
    completeness = importlib.import_module("silver.01_quality_completeness")
    df = completeness.validate_orders_completeness(df)
    df = type_validation.validate_orders_type_rules(df)
    row = df.collect()[0]
    assert row.dq_status == DQ_STATUS_PASS
    assert row.dq_failure_reasons is None or len(row.dq_failure_reasons) == 0


def test_invalid_fk_refs_normalize_to_integers(spark: SparkSession, tmp_path):
    """Invalid FK references normalize as integers; RI validation is a later iteration."""
    generate_all(seed=42, output_dir=str(tmp_path))
    config = BronzeConfig(
        catalog=None, schema_name="t", storage_path=None, input_dir=tmp_path
    )
    bronze = prepare_bronze_dataframe(
        spark, "orders", config, "b1", datetime(2026, 8, 16, tzinfo=timezone.utc)
    )
    silver = apply_silver_foundation("orders", bronze)
    invalid_cust_ids = [
        INVALID_CUSTOMER_ID_START + i for i in range(INVALID_ORDER_CUSTOMER_ID_COUNT)
    ]
    invalid_prod_ids = [
        INVALID_PRODUCT_ID_START + i for i in range(INVALID_ORDER_PRODUCT_ID_COUNT)
    ]
    assert silver.filter(F.col("customer_id").isin(invalid_cust_ids)).count() == INVALID_ORDER_CUSTOMER_ID_COUNT
    assert silver.filter(F.col("product_id").isin(invalid_prod_ids)).count() == INVALID_ORDER_PRODUCT_ID_COUNT
