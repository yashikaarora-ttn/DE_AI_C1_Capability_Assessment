"""Static and lightweight validation for dashboard SQL assets."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "src" / "dashboard"
sys.path.insert(0, str(ROOT / "src"))

REQUIRED_SQL_FILES = {
    "01_top_10_products_by_revenue.sql": {
        "gold_table": "gold_sales_by_product",
        "required_columns": [
            "product_id",
            "product_name",
            "category",
            "total_orders",
            "total_revenue",
            "avg_order_value",
        ],
    },
    "02_customer_revenue_distribution.sql": {
        "gold_table": "gold_revenue_by_customer",
        "required_columns": [
            "revenue_band",
            "customer_count",
            "total_revenue",
            "avg_customer_revenue",
        ],
    },
    "03_customer_segmentation.sql": {
        "gold_table": "gold_customer_segmentation",
        "required_columns": [
            "segment_type",
            "customer_count",
            "avg_revenue",
            "total_revenue",
        ],
    },
    "04_revenue_trend.sql": {
        "gold_table": "gold_daily_weekly_trends",
        "required_columns": [
            "period_start",
            "total_orders",
            "total_revenue",
            "avg_order_value",
        ],
    },
}

GOLD_TABLE_PATTERN = re.compile(r"gold_(sales_by_product|revenue_by_customer|customer_segmentation|daily_weekly_trends)")
FORBIDDEN_TABLE_PATTERN = re.compile(
    r"\b(bronze_|silver_)[a-z_]+\b", re.IGNORECASE
)


def _read_sql(filename: str) -> str:
    path = DASHBOARD_DIR / filename
    assert path.exists(), f"Missing dashboard SQL file: {filename}"
    content = path.read_text(encoding="utf-8")
    assert content.strip(), f"Empty dashboard SQL file: {filename}"
    return content


@pytest.mark.parametrize("filename", REQUIRED_SQL_FILES.keys())
def test_dashboard_sql_files_exist_and_non_empty(filename: str):
    _read_sql(filename)


@pytest.mark.parametrize("filename,spec", REQUIRED_SQL_FILES.items())
def test_dashboard_queries_reference_gold_tables_only(filename: str, spec: dict):
    sql = _read_sql(filename)
    assert spec["gold_table"] in sql
    assert GOLD_TABLE_PATTERN.search(sql), f"{filename} must reference a Gold table"
    forbidden = FORBIDDEN_TABLE_PATTERN.findall(sql)
    assert not forbidden, f"{filename} must not reference Bronze/Silver: {forbidden}"


@pytest.mark.parametrize("filename,spec", REQUIRED_SQL_FILES.items())
def test_dashboard_queries_include_required_columns(filename: str, spec: dict):
    sql = _read_sql(filename).lower()
    for column in spec["required_columns"]:
        assert column in sql, f"{filename} missing column alias/reference: {column}"


def test_top_10_query_orders_by_revenue_and_limits():
    sql = _read_sql("01_top_10_products_by_revenue.sql").lower()
    normalized = sql.replace("\n", " ")
    assert "order by total_revenue desc" in normalized
    assert "product_id" in normalized
    assert re.search(r"limit\s+10\b", sql)


def test_top_10_has_deterministic_tie_break():
    sql = _read_sql("01_top_10_products_by_revenue.sql").lower()
    normalized = sql.replace("\n", " ")
    assert "product_id asc" in normalized


def test_segmentation_query_consumes_gold_table_only():
    sql = _read_sql("03_customer_segmentation.sql").lower()
    assert "gold_customer_segmentation" in sql
    assert "from ${schema}.gold_customer_segmentation" in sql.replace("\n", " ").replace("  ", " ")
    assert "when total_orders" not in sql
    assert "high_value" not in sql
    assert "gold_revenue_by_customer" not in sql


def test_trend_query_uses_weekly_gold_trend():
    sql = _read_sql("04_revenue_trend.sql")
    assert "gold_daily_weekly_trends" in sql
    assert "WEEKLY" in sql
    assert "period_start" in sql.lower()
    assert "order by period_start" in sql.lower().replace("\n", " ")


def test_revenue_distribution_has_mutually_exclusive_bands():
    sql = _read_sql("02_customer_revenue_distribution.sql")
    for band in ("No Revenue", "Low", "Medium", "High", "Very High"):
        assert band in sql


@pytest.mark.parametrize("filename", REQUIRED_SQL_FILES.keys())
def test_dashboard_queries_use_schema_placeholder(filename: str):
    sql = _read_sql(filename)
    assert "${schema}" in sql


class TestDashboardSourceDataSupport:
    """Lightweight PySpark check that Gold outputs support dashboard shapes."""

    @pytest.fixture
    def gold_dfs(self, spark, tmp_path):
        pytest.importorskip("pyspark")
        from bronze.bronze_common import BronzeConfig, prepare_bronze_dataframe
        from data_generation.generate_sample_data import generate_all
        from gold.create_gold_tables import build_all_gold_dfs
        from gold.gold_config import GoldConfig
        from silver.silver_foundation import apply_silver_all

        generate_all(seed=42, output_dir=str(tmp_path))
        bronze_config = BronzeConfig(
            catalog=None,
            schema_name="dashboard_test",
            storage_path=None,
            input_dir=tmp_path,
        )
        batch_id = "dashboard-test"
        ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        bronze_dfs = {
            entity: prepare_bronze_dataframe(spark, entity, bronze_config, batch_id, ts)
            for entity in ("customers", "products", "orders")
        }
        silver_dfs = apply_silver_all(bronze_dfs)
        config = GoldConfig(
            catalog=None,
            schema_name="dashboard_test",
            storage_path=None,
        )
        return build_all_gold_dfs(silver_dfs, config)

    def test_top_10_source_has_enough_products(self, gold_dfs):
        count = gold_dfs["sales_by_product"].count()
        assert count >= 10

    def test_revenue_distribution_covers_all_customers(self, gold_dfs):
        from pyspark.sql import functions as F

        customers = gold_dfs["revenue_by_customer"]
        total_customers = customers.count()
        # Simulate band assignment (same rules as dashboard SQL)
        banded = customers.withColumn(
            "revenue_band",
            F.when(F.col("total_revenue") == 0, "No Revenue")
            .when(
                (F.col("total_revenue") > 0) & (F.col("total_revenue") < 500),
                "Low",
            )
            .when(
                (F.col("total_revenue") >= 500) & (F.col("total_revenue") < 2000),
                "Medium",
            )
            .when(
                (F.col("total_revenue") >= 2000) & (F.col("total_revenue") < 5000),
                "High",
            )
            .when(F.col("total_revenue") >= 5000, "Very High"),
        )
        banded_count = banded.groupBy("revenue_band").count().agg(
            F.sum("count")
        ).collect()[0][0]
        assert banded_count == total_customers

    def test_segmentation_source_has_segments(self, gold_dfs):
        segments = gold_dfs["customer_segmentation"].collect()
        segment_types = {r.segment_type for r in segments}
        assert "High-Value" in segment_types
        assert len(segments) >= 3

    def test_weekly_trend_source_has_rows(self, gold_dfs):
        from gold.gold_common import PERIOD_WEEKLY
        from pyspark.sql import functions as F

        weekly = gold_dfs["daily_weekly_trends"].filter(
            F.col("period_type") == PERIOD_WEEKLY
        )
        assert weekly.count() > 0
        assert weekly.orderBy("period_start").limit(1).count() == 1
