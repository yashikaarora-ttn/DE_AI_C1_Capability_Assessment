"""Tests for Gold layer aggregations and trusted-data policy."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from bronze.bronze_common import (  # noqa: E402
    BronzeConfig,
    CUSTOMERS_CSV_SCHEMA,
    ORDERS_CSV_SCHEMA,
    PRODUCTS_CSV_SCHEMA,
    prepare_bronze_dataframe,
)
from data_generation.generate_sample_data import generate_all  # noqa: E402
from gold.create_gold_tables import build_all_gold_dfs  # noqa: E402
from gold.gold_common import (  # noqa: E402
    COMPLETED_ORDER_STATUS,
    PERIOD_DAILY,
    PERIOD_WEEKLY,
    SEGMENT_HIGH_VALUE,
    SEGMENT_INACTIVE,
    SEGMENT_ONE_TIME,
    SEGMENT_REPEAT,
    build_customer_segmentation,
    build_daily_weekly_trends,
    build_revenue_by_customer,
    build_sales_by_product,
    total_trusted_business_revenue,
    trusted_business_orders,
    trusted_completed_orders,
    trusted_silver,
)
from gold.gold_config import DEFAULT_HIGH_VALUE_THRESHOLD, GoldConfig  # noqa: E402
from silver.silver_common import DQ_STATUS_FAIL, DQ_STATUS_PASS  # noqa: E402
from silver.silver_foundation import apply_silver_all  # noqa: E402

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _silver_customer(
    customer_id: int,
    name: str = "Cust",
    segment: str = "Basic",
    dq_status: str = DQ_STATUS_PASS,
) -> tuple:
    return (
        customer_id,
        name,
        f"{name.lower()}@example.com",
        "US",
        date(2020, 1, 1),
        segment,
        Decimal("50.00"),
        dq_status,
        [],
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        "file.csv",
        "batch-1",
        datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


def _silver_product(
    product_id: int,
    name: str = "Prod",
    category: str = "Cat",
    dq_status: str = DQ_STATUS_PASS,
) -> tuple:
    return (
        product_id,
        name,
        category,
        Decimal("10.00"),
        Decimal("5.00"),
        100,
        10,
        dq_status,
        [],
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        "file.csv",
        "batch-1",
        datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


def _silver_order(
    order_id: int,
    customer_id: int,
    product_id: int,
    total_amount: Decimal,
    order_status: str = COMPLETED_ORDER_STATUS,
    order_date: date = date(2020, 1, 15),
    dq_status: str = DQ_STATUS_PASS,
) -> tuple:
    return (
        order_id,
        customer_id,
        order_date,
        product_id,
        1,
        Decimal("10.00"),
        total_amount,
        order_status,
        date(2020, 1, 16),
        dq_status,
        [],
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        "file.csv",
        "batch-1",
        datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


CUSTOMER_SILVER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType()),
        StructField("customer_name", StringType()),
        StructField("email", StringType()),
        StructField("country", StringType()),
        StructField("signup_date", DateType()),
        StructField("customer_segment", StringType()),
        StructField("lifetime_value", DecimalType(12, 2)),
        StructField("dq_status", StringType()),
        StructField("dq_failure_reasons", ArrayType(StringType())),
        StructField("_ingestion_timestamp", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_silver_processed_at", TimestampType()),
    ]
)

PRODUCT_SILVER_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("price", DecimalType(10, 2)),
        StructField("cost", DecimalType(10, 2)),
        StructField("stock_quantity", IntegerType()),
        StructField("reorder_level", IntegerType()),
        StructField("dq_status", StringType()),
        StructField("dq_failure_reasons", ArrayType(StringType())),
        StructField("_ingestion_timestamp", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_silver_processed_at", TimestampType()),
    ]
)

ORDER_SILVER_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("order_date", DateType()),
        StructField("product_id", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("total_amount", DecimalType(12, 2)),
        StructField("order_status", StringType()),
        StructField("payment_date", DateType()),
        StructField("dq_status", StringType()),
        StructField("dq_failure_reasons", ArrayType(StringType())),
        StructField("_ingestion_timestamp", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_silver_processed_at", TimestampType()),
    ]
)


def _make_silver_dfs(spark: SparkSession, customers, products, orders):
    return {
        "customers": spark.createDataFrame(customers, CUSTOMER_SILVER_SCHEMA),
        "products": spark.createDataFrame(products, PRODUCT_SILVER_SCHEMA),
        "orders": spark.createDataFrame(orders, ORDER_SILVER_SCHEMA),
    }


@pytest.fixture
def gold_config() -> GoldConfig:
    return GoldConfig(
        catalog=None,
        schema_name="gold_test",
        storage_path=None,
        high_value_threshold=100.0,
    )


@pytest.fixture
def fixture_silver(spark: SparkSession):
    """Small deterministic Silver fixture for aggregation tests."""
    customers = [
        _silver_customer(1, "Alice", "Premium"),
        _silver_customer(2, "Bob", "Basic"),
        _silver_customer(3, "Carol", "Standard", DQ_STATUS_FAIL),
        _silver_customer(4, "Dave", "Basic"),
    ]
    products = [
        _silver_product(10, "Widget", "Tools"),
        _silver_product(20, "Gadget", "Electronics"),
        _silver_product(30, "BadProd", "Tools", DQ_STATUS_FAIL),
    ]
    orders = [
        _silver_order(1, 1, 10, Decimal("50.00")),
        _silver_order(2, 1, 10, Decimal("30.00")),
        _silver_order(3, 2, 20, Decimal("40.00")),
        _silver_order(4, 2, 20, Decimal("20.00"), order_status="Pending"),
        _silver_order(5, 3, 10, Decimal("99.00"), dq_status=DQ_STATUS_FAIL),
        _silver_order(6, 4, 30, Decimal("25.00")),
        _silver_order(7, 1, 20, Decimal("10.00"), order_status="Cancelled"),
    ]
    return _make_silver_dfs(spark, customers, products, orders)


class TestTrustedFiltering:
    def test_fail_orders_excluded_from_completed(self, spark, fixture_silver):
        completed = trusted_completed_orders(fixture_silver["orders"])
        ids = {r.order_id for r in completed.collect()}
        assert ids == {1, 2, 3, 6}

    def test_fail_products_excluded_from_sales(self, spark, fixture_silver):
        sales = build_sales_by_product(
            fixture_silver["orders"],
            fixture_silver["products"],
            fixture_silver["customers"],
        )
        product_ids = {r.product_id for r in sales.collect()}
        assert 30 not in product_ids

    def test_fail_customers_excluded_from_revenue(self, spark, fixture_silver):
        revenue = build_revenue_by_customer(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        )
        ids = {r.customer_id for r in revenue.collect()}
        assert ids == {1, 2, 4}

    def test_non_completed_excluded_from_revenue(self, spark, fixture_silver):
        business = trusted_business_orders(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        )
        base_revenue = float(
            business.agg(F.sum("total_amount").alias("r")).collect()[0].r
        )
        assert base_revenue == 120.0  # 50+30+40 only


class TestSalesByProduct:
    def test_totals_and_grouping(self, spark, fixture_silver):
        sales = build_sales_by_product(
            fixture_silver["orders"],
            fixture_silver["products"],
            fixture_silver["customers"],
        ).collect()
        by_id = {r.product_id: r for r in sales}
        assert len(by_id) == 2
        assert by_id[10].total_orders == 2
        assert float(by_id[10].total_revenue) == 80.0
        assert float(by_id[10].avg_order_value) == 40.0
        assert by_id[20].total_orders == 1
        assert float(by_id[20].total_revenue) == 40.0

    def test_no_double_counting(self, spark, fixture_silver):
        sales = build_sales_by_product(
            fixture_silver["orders"],
            fixture_silver["products"],
            fixture_silver["customers"],
        )
        order_sum = sales.agg(F.sum("total_orders").alias("n")).collect()[0].n
        business_count = trusted_business_orders(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        ).count()
        assert order_sum == business_count


class TestRevenueByCustomer:
    def test_totals(self, spark, fixture_silver):
        revenue = build_revenue_by_customer(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        ).collect()
        by_id = {r.customer_id: r for r in revenue}
        assert by_id[1].total_orders == 2
        assert float(by_id[1].total_revenue) == 80.0
        assert float(by_id[1].avg_order_value) == 40.0
        assert float(by_id[1].lifetime_value_actual) == 80.0
        assert by_id[2].total_orders == 1
        assert float(by_id[2].total_revenue) == 40.0

    def test_no_completed_orders_customer(self, spark, fixture_silver):
        revenue = build_revenue_by_customer(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        )
        dave = revenue.filter(F.col("customer_id") == 4).collect()[0]
        assert dave.total_orders == 0
        assert float(dave.total_revenue) == 0.0
        assert float(dave.avg_order_value) == 0.0
        assert float(dave.lifetime_value_actual) == 0.0


class TestSegmentation:
    def test_segment_rules(self, spark, fixture_silver, gold_config):
        segments = build_customer_segmentation(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
            gold_config.high_value_threshold,
        ).collect()
        by_seg = {r.segment_type: r for r in segments}
        assert by_seg[SEGMENT_REPEAT].customer_count == 1  # Alice: 2 orders, 80 revenue < 100
        assert by_seg[SEGMENT_ONE_TIME].customer_count == 1  # Bob
        assert by_seg[SEGMENT_INACTIVE].customer_count == 1  # Dave

    def test_high_value_segment_rule(self, spark, fixture_silver):
        segments = build_customer_segmentation(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
            high_value_threshold=50.0,
        ).collect()
        by_seg = {r.segment_type: r for r in segments}
        assert by_seg[SEGMENT_HIGH_VALUE].customer_count == 1  # Alice: 2 orders, 80 revenue

    def test_mutually_exclusive_and_reconcile(self, spark, fixture_silver, gold_config):
        segments = build_customer_segmentation(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
            gold_config.high_value_threshold,
        )
        total_segmented = segments.agg(F.sum("customer_count")).collect()[0][0]
        trusted_customers = trusted_silver(fixture_silver["customers"]).count()
        assert total_segmented == trusted_customers
        assert segments.count() == 3


class TestTrends:
    def test_daily_and_weekly(self, spark, fixture_silver):
        trends = build_daily_weekly_trends(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        ).collect()
        daily = [t for t in trends if t.period_type == PERIOD_DAILY]
        weekly = [t for t in trends if t.period_type == PERIOD_WEEKLY]
        assert len(daily) == 1
        assert daily[0].total_orders == 3
        assert float(daily[0].total_revenue) == 120.0
        assert len(weekly) == 1
        assert weekly[0].total_orders == 3

    def test_completed_only(self, spark, fixture_silver):
        trends = build_daily_weekly_trends(
            fixture_silver["orders"],
            fixture_silver["customers"],
            fixture_silver["products"],
        )
        total_rev = trends.agg(F.sum("total_revenue")).collect()[0][0]
        assert float(total_rev) == 120.0 + 120.0  # daily + weekly same orders


class TestOutputContracts:
    def test_required_columns(self, spark, fixture_silver, gold_config):
        gold = build_all_gold_dfs(fixture_silver, gold_config)
        assert set(gold["sales_by_product"].columns) >= {
            "product_id",
            "product_name",
            "category",
            "total_orders",
            "total_revenue",
            "avg_order_value",
        }
        assert set(gold["revenue_by_customer"].columns) >= {
            "customer_id",
            "customer_name",
            "customer_segment",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "lifetime_value_actual",
        }
        assert set(gold["daily_weekly_trends"].columns) >= {
            "period_type",
            "period_start",
            "total_orders",
            "total_revenue",
            "avg_order_value",
        }
        assert set(gold["customer_segmentation"].columns) >= {
            "segment_type",
            "customer_count",
            "avg_revenue",
            "total_revenue",
        }

    def test_numeric_types(self, spark, fixture_silver, gold_config):
        gold = build_all_gold_dfs(fixture_silver, gold_config)
        row = gold["sales_by_product"].collect()[0]
        assert isinstance(row.total_orders, int)
        assert row.total_revenue is not None


@pytest.fixture
def generated_silver(spark: SparkSession, tmp_path):
    generate_all(seed=42, output_dir=str(tmp_path))
    bronze_config = BronzeConfig(
        catalog=None,
        schema_name="gold_e2e",
        storage_path=None,
        input_dir=tmp_path,
    )
    batch_id = "gold-e2e"
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    bronze_dfs = {
        entity: prepare_bronze_dataframe(spark, entity, bronze_config, batch_id, ts)
        for entity in ("customers", "products", "orders")
    }
    return apply_silver_all(bronze_dfs)


class TestGeneratedDataValidation:
    def test_reconciliation_and_counts(self, spark, generated_silver):
        config = GoldConfig(
            catalog=None,
            schema_name="gold_e2e",
            storage_path=None,
            high_value_threshold=DEFAULT_HIGH_VALUE_THRESHOLD,
        )
        gold = build_all_gold_dfs(generated_silver, config)
        orders = generated_silver["orders"]
        customers = generated_silver["customers"]
        products = generated_silver["products"]

        trusted_pass_customers = trusted_silver(customers).count()
        trusted_pass_products = trusted_silver(products).count()
        trusted_pass_orders = trusted_silver(orders).count()
        trusted_completed_count = trusted_business_orders(
            orders, customers, products
        ).count()
        trusted_revenue = total_trusted_business_revenue(orders, customers, products)

        product_rev_sum = float(
            gold["sales_by_product"].agg(F.sum("total_revenue")).collect()[0][0]
        )
        customer_rev_sum = float(
            gold["revenue_by_customer"].agg(F.sum("total_revenue")).collect()[0][0]
        )

        assert product_rev_sum == trusted_revenue
        assert customer_rev_sum == trusted_revenue

        sales_rows = gold["sales_by_product"].count()
        customer_rows = gold["revenue_by_customer"].count()
        assert customer_rows == trusted_pass_customers

        seg_total = gold["customer_segmentation"].agg(
            F.sum("customer_count")
        ).collect()[0][0]
        assert seg_total == trusted_pass_customers

        daily_count = gold["daily_weekly_trends"].filter(
            F.col("period_type") == PERIOD_DAILY
        ).count()
        weekly_count = gold["daily_weekly_trends"].filter(
            F.col("period_type") == PERIOD_WEEKLY
        ).count()
        assert daily_count > 0
        assert weekly_count > 0

        # Store metrics on spark for reporting (session-scoped)
        spark.conf.set("gold.validation.trusted_pass_customers", str(trusted_pass_customers))
        spark.conf.set("gold.validation.trusted_pass_products", str(trusted_pass_products))
        spark.conf.set("gold.validation.trusted_pass_orders", str(trusted_pass_orders))
        spark.conf.set(
            "gold.validation.trusted_business_orders", str(trusted_completed_count)
        )
        spark.conf.set("gold.validation.trusted_revenue", str(trusted_revenue))
        spark.conf.set("gold.validation.sales_rows", str(sales_rows))
        spark.conf.set("gold.validation.customer_rows", str(customer_rows))
        spark.conf.set("gold.validation.daily_rows", str(daily_count))
        spark.conf.set("gold.validation.weekly_rows", str(weekly_count))
