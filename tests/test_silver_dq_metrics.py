"""Tests for Silver DQ metrics builder."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from bronze.bronze_common import CUSTOMERS_CSV_SCHEMA, ORDERS_CSV_SCHEMA, PRODUCTS_CSV_SCHEMA  # noqa: E402
from silver.dq_metrics import (  # noqa: E402
    METRIC_TYPE_OVERALL,
    METRIC_TYPE_RULE,
    build_dq_metrics_df,
    build_entity_metrics,
    count_reason_failures,
    get_overall_metrics,
)
from silver.silver_common import (  # noqa: E402
    DQ_STATUS_FAIL,
    DUPLICATE_CUSTOMER_ID,
    NULL_EMAIL,
)
from silver.silver_foundation import apply_silver_all  # noqa: E402
from bronze.bronze_common import BronzeConfig, prepare_bronze_dataframe  # noqa: E402
from data_generation.generate_sample_data import generate_all  # noqa: E402

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture
def sample_data_dir(tmp_path) -> Path:
    generate_all(seed=42, output_dir=str(tmp_path))
    return tmp_path


def test_rule_metrics_zero_failure_rule(spark: SparkSession):
    rows = [(1, "A", "Cat", Decimal("10.00"), Decimal("5.00"), 100, 10)]
    df = spark.createDataFrame(rows, schema=PRODUCTS_CSV_SCHEMA)
    from importlib import import_module
    from silver.silver_common import DUPLICATE_PRODUCT_ID, finalize_dq_columns

    uniqueness = import_module("silver.02_quality_uniqueness")
    df = finalize_dq_columns(uniqueness.validate_products_uniqueness(df))

    metrics = build_entity_metrics(
        df, "products", "batch_test", datetime(2026, 8, 16, tzinfo=timezone.utc)
    )
    dup_row = next(r for r in metrics if r["reason_code"] == DUPLICATE_PRODUCT_ID)
    assert dup_row["failed_count"] == 0
    assert dup_row["failed_percentage"] == 0.0
    assert dup_row["passed_percentage"] == 100.0


def test_overall_fail_not_inflated_by_multiple_reasons(spark: SparkSession):
    """Row with two reasons should count once in OVERALL FAIL, twice in RULE metrics."""
    order_row = (
        1,
        None,
        date(2020, 1, 1),
        None,
        0,
        Decimal("10.00"),
        Decimal("99.00"),
        "BadStatus",
        None,
    )
    df = spark.createDataFrame([order_row], schema=ORDERS_CSV_SCHEMA)
    from importlib import import_module

    completeness = import_module("silver.01_quality_completeness")
    type_validation = import_module("silver.03_quality_type_validation")
    df = completeness.validate_orders_completeness(df)
    df = type_validation.validate_orders_type_rules(df)
    from pyspark.sql import functions as F

    assert df.filter(F.col("dq_status") == DQ_STATUS_FAIL).count() == 1

    metrics = build_entity_metrics(
        df, "orders", "batch_m", datetime(2026, 8, 16, tzinfo=timezone.utc)
    )
    overall = next(r for r in metrics if r["metric_type"] == METRIC_TYPE_OVERALL)
    assert overall["failed_count"] == 1
    null_cust = next(r for r in metrics if r["reason_code"] == "NULL_CUSTOMER_ID")
    null_prod = next(r for r in metrics if r["reason_code"] == "NULL_PRODUCT_ID")
    assert null_cust["failed_count"] == 1
    assert null_prod["failed_count"] == 1


def test_batch_id_in_metrics(spark: SparkSession):
    rows = [
        (1, "Alice", None, "US", date(2020, 1, 1), "Basic", Decimal("100.00")),
    ]
    df = spark.createDataFrame(rows, schema=CUSTOMERS_CSV_SCHEMA)
    from importlib import import_module

    completeness = import_module("silver.01_quality_completeness")
    df = completeness.validate_customers_completeness(df)
    metrics = build_entity_metrics(
        df, "customers", "batch_xyz", datetime(2026, 8, 16, tzinfo=timezone.utc)
    )
    assert all(r["batch_id"] == "batch_xyz" for r in metrics)


def test_full_dataset_metrics_counts(spark: SparkSession, sample_data_dir: Path):
    config = BronzeConfig(
        catalog=None, schema_name="t", storage_path=None, input_dir=sample_data_dir
    )
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)
    bronze = {
        e: prepare_bronze_dataframe(spark, e, config, "batch_metrics", ts)
        for e in ("customers", "products", "orders")
    }
    silver = apply_silver_all(bronze)
    metrics_df = build_dq_metrics_df(spark, silver, "batch_metrics", ts)

    assert count_reason_failures(metrics_df, "customers", "NULL_EMAIL") == 50
    assert count_reason_failures(metrics_df, "customers", DUPLICATE_CUSTOMER_ID) == 10
    assert count_reason_failures(metrics_df, "products", "DUPLICATE_PRODUCT_ID") == 0
    assert count_reason_failures(metrics_df, "orders", "NULL_CUSTOMER_ID") == 100
    assert count_reason_failures(metrics_df, "orders", "NULL_PRODUCT_ID") == 200
    assert count_reason_failures(metrics_df, "orders", "INVALID_CUSTOMER_ID") == 50
    assert count_reason_failures(metrics_df, "orders", "INVALID_PRODUCT_ID") == 30
    assert count_reason_failures(metrics_df, "orders", "DUPLICATE_ORDER_ID") == 20

    cust_overall = get_overall_metrics(metrics_df, "customers")
    assert cust_overall["total_records"] == 10000
    assert cust_overall["passed_count"] == 9940
    assert cust_overall["failed_count"] == 60
    assert cust_overall["passed_percentage"] == 99.4

    prod_overall = get_overall_metrics(metrics_df, "products")
    assert prod_overall["total_records"] == 500
    assert prod_overall["failed_count"] == 0
    assert prod_overall["passed_percentage"] == 100.0

    ord_overall = get_overall_metrics(metrics_df, "orders")
    assert ord_overall["total_records"] == 100000
    assert ord_overall["passed_count"] == 99600
    assert ord_overall["failed_count"] == 400
    assert ord_overall["passed_percentage"] == 99.6
