"""Tests for Silver table creation orchestration."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from bronze.bronze_common import BronzeConfig  # noqa: E402
from data_generation.generate_sample_data import (  # noqa: E402
    NUM_CUSTOMERS,
    NUM_ORDERS,
    NUM_PRODUCTS,
    generate_all,
)
from silver.create_silver_tables import run_silver_pipeline  # noqa: E402
from silver.silver_config import (  # noqa: E402
    DQ_METRICS_TABLE,
    SILVER_ENTITY_TABLES,
    SilverConfig,
)
from silver.silver_common import DQ_STATUS_FAIL  # noqa: E402

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
        schema_name="silver_create_test",
        storage_path=None,
        input_dir=sample_data_dir,
    )


@pytest.fixture
def silver_config() -> SilverConfig:
    return SilverConfig(
        catalog=None,
        schema_name="silver_create_test",
        storage_path=None,
        read_bronze_from_delta=False,
        entity_write_mode="overwrite",
        metrics_write_mode="append",
    )


def test_pipeline_produces_all_entities(
    spark: SparkSession, bronze_config: BronzeConfig, silver_config: SilverConfig
):
    result = run_silver_pipeline(
        spark,
        bronze_config,
        silver_config,
        batch_id="batch_create",
        write_delta=False,
    )
    assert set(result.silver_dfs.keys()) == {"customers", "products", "orders"}
    assert result.silver_dfs["customers"].count() == NUM_CUSTOMERS
    assert result.silver_dfs["products"].count() == NUM_PRODUCTS
    assert result.silver_dfs["orders"].count() == NUM_ORDERS


def test_failed_rows_preserved(
    spark: SparkSession, bronze_config: BronzeConfig, silver_config: SilverConfig
):
    result = run_silver_pipeline(
        spark, bronze_config, silver_config, batch_id="b1", write_delta=False
    )
    fail_orders = result.silver_dfs["orders"].filter(F.col("dq_status") == DQ_STATUS_FAIL)
    assert fail_orders.count() == 400


def test_lineage_metadata_present(
    spark: SparkSession, bronze_config: BronzeConfig, silver_config: SilverConfig
):
    result = run_silver_pipeline(
        spark, bronze_config, silver_config, batch_id="b_lineage", write_delta=False
    )
    orders = result.silver_dfs["orders"]
    for col in ("_ingestion_timestamp", "_source_file", "_batch_id", "_silver_processed_at"):
        assert col in orders.columns


def test_write_configuration_table_names(
    spark: SparkSession,
    bronze_config: BronzeConfig,
    silver_config: SilverConfig,
    monkeypatch: pytest.MonkeyPatch,
):
    written: list[tuple[str, str]] = []

    def mock_write(spark, df, table_name, config, mode=None):
        written.append((table_name, mode or config.entity_write_mode))

    def mock_metrics(spark, metrics_df, config):
        written.append((DQ_METRICS_TABLE, config.metrics_write_mode))

    monkeypatch.setattr(
        "silver.create_silver_tables.write_silver_delta_table", mock_write
    )
    monkeypatch.setattr("silver.create_silver_tables.write_silver_metrics", mock_metrics)

    run_silver_pipeline(
        spark, bronze_config, silver_config, batch_id="b_write", write_delta=True
    )

    expected_entities = set(SILVER_ENTITY_TABLES.values())
    entity_writes = {t for t, m in written if t in expected_entities}
    assert entity_writes == expected_entities
    assert (DQ_METRICS_TABLE, "append") in written
    for table_name, mode in written:
        if table_name in expected_entities:
            assert mode == "overwrite"
