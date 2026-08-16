"""
Silver layer configuration and Delta write helpers.

Write strategy (assessment default)
------------------------------------
- Entity Silver tables (`silver_customers`, `silver_orders`, `silver_products`):
  **overwrite** — latest validated snapshot per entity (mirrors Bronze entity strategy).
- `silver_dq_metrics`: **append** — retain metrics history per batch/run.

Trade-off: overwrite keeps one current Silver snapshot for simple dev/Gold consumption;
metrics append preserves audit trail of pass/fail percentages across runs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from bronze.bronze_common import find_repo_root

SILVER_ENTITY_TABLES: Dict[str, str] = {
    "customers": "silver_customers",
    "orders": "silver_orders",
    "products": "silver_products",
}

DQ_METRICS_TABLE = "silver_dq_metrics"

BRONZE_TABLE_NAMES: Dict[str, str] = {
    "customers": "bronze_customers",
    "orders": "bronze_orders",
    "products": "bronze_products",
}


class SilverWriteError(Exception):
    """Failed to write Silver Delta table or metrics."""


@dataclass
class SilverConfig:
    """Runtime configuration for Silver processing (env-driven)."""

    catalog: Optional[str]
    schema_name: str
    storage_path: Optional[str]
    entity_write_mode: str = "overwrite"
    metrics_write_mode: str = "append"
    read_bronze_from_delta: bool = False

    @classmethod
    def from_env(cls, repo_root: Optional[Path] = None) -> "SilverConfig":
        root = repo_root or find_repo_root()
        schema = os.environ.get("SILVER_SCHEMA") or os.environ.get(
            "BRONZE_SCHEMA", "ecommerce_medallion"
        )
        catalog = os.environ.get("SILVER_CATALOG") or os.environ.get("BRONZE_CATALOG") or None
        storage = os.environ.get("SILVER_STORAGE_PATH") or os.environ.get(
            "BRONZE_STORAGE_PATH"
        ) or None
        read_delta = os.environ.get("SILVER_READ_BRONZE_DELTA", "").lower() in (
            "1",
            "true",
            "yes",
        )
        return cls(
            catalog=catalog,
            schema_name=schema,
            storage_path=storage,
            entity_write_mode=os.environ.get("SILVER_ENTITY_WRITE_MODE", "overwrite"),
            metrics_write_mode=os.environ.get("SILVER_METRICS_WRITE_MODE", "append"),
            read_bronze_from_delta=read_delta,
        )

    def qualified_table_name(self, table_name: str) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema_name}.{table_name}"
        return f"{self.schema_name}.{table_name}"

    def table_storage_path(self, table_name: str) -> Optional[str]:
        if not self.storage_path:
            return None
        return f"{self.storage_path.rstrip('/')}/silver/{table_name}"


def write_silver_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    config: SilverConfig,
    mode: Optional[str] = None,
) -> None:
    """Write a Silver DataFrame to Delta (Databricks or local metastore)."""
    write_mode = mode or config.entity_write_mode
    qualified = config.qualified_table_name(table_name)
    storage_path = config.table_storage_path(table_name)

    try:
        writer = df.write.format("delta").mode(write_mode)
        if storage_path:
            writer = writer.option("path", storage_path)
        if config.catalog:
            writer.saveAsTable(qualified)
        else:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.schema_name}")
            writer.saveAsTable(qualified)
    except Exception as exc:
        raise SilverWriteError(
            f"Failed to write Delta table '{qualified}' (mode={write_mode}): {exc}"
        ) from exc


def write_silver_metrics(
    spark: SparkSession,
    metrics_df: DataFrame,
    config: SilverConfig,
) -> None:
    write_silver_delta_table(
        spark,
        metrics_df,
        DQ_METRICS_TABLE,
        config,
        mode=config.metrics_write_mode,
    )
