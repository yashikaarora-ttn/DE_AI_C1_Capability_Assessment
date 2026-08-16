"""Tests for Silver uniqueness validation."""

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

from bronze.bronze_common import CUSTOMERS_CSV_SCHEMA, ORDERS_CSV_SCHEMA, BronzeConfig, prepare_bronze_dataframe  # noqa: E402
from data_generation.generate_sample_data import (  # noqa: E402
    DUPLICATE_CUSTOMER_ID_ROWS,
    DUPLICATE_ORDER_ID_ROWS,
    generate_all,
)
from silver.silver_common import (  # noqa: E402
    DQ_STATUS_PASS,
    DUPLICATE_CUSTOMER_ID,
    DUPLICATE_ORDER_ID,
    DUPLICATE_PRODUCT_ID,
    NULL_EMAIL,
)
from silver.silver_foundation import apply_silver_validation  # noqa: E402

uniqueness = importlib.import_module("silver.02_quality_uniqueness")
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


def test_unique_customer_key_passes_when_no_other_failures(spark: SparkSession):
    row = (
        1,
        "Alice",
        "alice@example.com",
        "US",
        date(2020, 1, 1),
        "Basic",
        Decimal("100.00"),
    )
    df = spark.createDataFrame([row], schema=CUSTOMERS_CSV_SCHEMA)
    df = completeness.validate_customers_completeness(df)
    df = uniqueness.validate_customers_uniqueness(df)
    result = df.collect()[0]
    assert result.dq_status == DQ_STATUS_PASS
    assert DUPLICATE_CUSTOMER_ID not in (result.dq_failure_reasons or [])


def test_all_duplicate_customer_rows_flagged(spark: SparkSession):
    rows = [
        (1, "Alice", "a1@example.com", "US", date(2020, 1, 1), "Basic", Decimal("100.00")),
        (1, "Bob", "b1@example.com", "US", date(2020, 1, 2), "Basic", Decimal("200.00")),
    ]
    df = spark.createDataFrame(rows, schema=CUSTOMERS_CSV_SCHEMA)
    df = uniqueness.validate_customers_uniqueness(df)
    results = df.collect()
    assert len(results) == 2
    for row in results:
        assert DUPLICATE_CUSTOMER_ID in row.dq_failure_reasons


def test_all_duplicate_order_rows_flagged(spark: SparkSession):
    rows = [
        (
            1,
            "100",
            date(2020, 1, 1),
            "10",
            1,
            Decimal("10.00"),
            Decimal("10.00"),
            "Completed",
            date(2020, 1, 2),
        ),
        (
            1,
            "101",
            date(2020, 1, 2),
            "11",
            2,
            Decimal("20.00"),
            Decimal("40.00"),
            "Completed",
            date(2020, 1, 3),
        ),
    ]
    df = spark.createDataFrame(rows, schema=ORDERS_CSV_SCHEMA)
    df = uniqueness.validate_orders_uniqueness(df)
    results = df.collect()
    assert len(results) == 2
    for row in results:
        assert DUPLICATE_ORDER_ID in row.dq_failure_reasons


def test_product_uniqueness_no_false_positives(spark: SparkSession):
    from bronze.bronze_common import PRODUCTS_CSV_SCHEMA
    from pyspark.sql import functions as F

    rows = [
        (1, "A", "Cat", Decimal("10.00"), Decimal("5.00"), 100, 10),
        (2, "B", "Cat", Decimal("20.00"), Decimal("10.00"), 50, 5),
    ]
    df = spark.createDataFrame(rows, schema=PRODUCTS_CSV_SCHEMA)
    df = uniqueness.validate_products_uniqueness(df)
    assert df.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_PRODUCT_ID)
    ).count() == 0


def test_previous_reason_codes_preserved_after_uniqueness(spark: SparkSession):
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
    df = completeness.validate_customers_completeness(df)
    df = uniqueness.validate_customers_uniqueness(df)
    result = df.collect()[0]
    assert NULL_EMAIL in result.dq_failure_reasons


def test_uniqueness_does_not_delete_rows(spark: SparkSession):
    rows = [
        (1, "Alice", "a@b.com", "US", date(2020, 1, 1), "Basic", Decimal("100.00")),
        (1, "Bob", "b@b.com", "US", date(2020, 1, 2), "Basic", Decimal("200.00")),
        (2, "Carol", "c@b.com", "US", date(2020, 1, 3), "Basic", Decimal("300.00")),
    ]
    df = spark.createDataFrame(rows, schema=CUSTOMERS_CSV_SCHEMA)
    result = uniqueness.validate_customers_uniqueness(df)
    assert result.count() == 3


def test_generated_data_duplicate_counts(
    spark: SparkSession, bronze_config: BronzeConfig
):
    from pyspark.sql import functions as F

    batch_id = "uniq_test"
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)
    bronze = prepare_bronze_dataframe(spark, "customers", bronze_config, batch_id, ts)
    silver = apply_silver_validation("customers", bronze)
    dup = silver.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_CUSTOMER_ID)
    ).count()
    assert dup == DUPLICATE_CUSTOMER_ID_ROWS

    bronze_orders = prepare_bronze_dataframe(spark, "orders", bronze_config, batch_id, ts)
    type_validation = importlib.import_module("silver.03_quality_type_validation")
    orders_df = completeness.apply_completeness_validation("orders", bronze_orders)
    orders_df = type_validation.apply_type_validation("orders", orders_df)
    orders_df = type_validation.drop_order_fk_raw_columns(orders_df)
    silver_orders = uniqueness.validate_orders_uniqueness(orders_df)
    dup_orders = silver_orders.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_ORDER_ID)
    ).count()
    assert dup_orders == DUPLICATE_ORDER_ID_ROWS
