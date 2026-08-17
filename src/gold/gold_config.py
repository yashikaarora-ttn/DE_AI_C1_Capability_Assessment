"""
Gold layer configuration and Delta write helpers.

Write strategy: overwrite Gold snapshot tables (assessment default), consistent with
Bronze/Silver entity overwrite for latest business-ready metrics.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from bronze.bronze_common import find_repo_root

GOLD_TABLES: Dict[str, str] = {
    "sales_by_product": "gold_sales_by_product",
    "revenue_by_customer": "gold_revenue_by_customer",
    "daily_weekly_trends": "gold_daily_weekly_trends",
    "customer_segmentation": "gold_customer_segmentation",
}

DEFAULT_HIGH_VALUE_THRESHOLD = 1000.0
# Assessment default for High-Value segmentation — configure via GOLD_HIGH_VALUE_THRESHOLD;
# not a universal business rule.


class GoldWriteError(Exception):
    """Failed to write Gold Delta table."""


@dataclass
class GoldConfig:
    """Runtime configuration for Gold aggregations (env-driven)."""

    catalog: Optional[str]
    schema_name: str
    storage_path: Optional[str]
    write_mode: str = "overwrite"
    high_value_threshold: float = DEFAULT_HIGH_VALUE_THRESHOLD

    @classmethod
    def from_env(cls, repo_root: Optional[Path] = None) -> "GoldConfig":
        _ = repo_root or find_repo_root()
        schema = os.environ.get("GOLD_SCHEMA") or os.environ.get(
            "SILVER_SCHEMA", os.environ.get("BRONZE_SCHEMA", "ecommerce_medallion")
        )
        catalog = (
            os.environ.get("GOLD_CATALOG")
            or os.environ.get("SILVER_CATALOG")
            or os.environ.get("BRONZE_CATALOG")
            or None
        )
        storage = (
            os.environ.get("GOLD_STORAGE_PATH")
            or os.environ.get("SILVER_STORAGE_PATH")
            or os.environ.get("BRONZE_STORAGE_PATH")
            or None
        )
        threshold_raw = os.environ.get(
            "GOLD_HIGH_VALUE_THRESHOLD", str(DEFAULT_HIGH_VALUE_THRESHOLD)
        )
        return cls(
            catalog=catalog,
            schema_name=schema,
            storage_path=storage,
            write_mode=os.environ.get("GOLD_WRITE_MODE", "overwrite"),
            high_value_threshold=float(threshold_raw),
        )

    def qualified_table_name(self, table_name: str) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema_name}.{table_name}"
        return f"{self.schema_name}.{table_name}"

    def table_storage_path(self, table_name: str) -> Optional[str]:
        if not self.storage_path:
            return None
        return f"{self.storage_path.rstrip('/')}/gold/{table_name}"


def write_gold_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    config: GoldConfig,
) -> None:
    qualified = config.qualified_table_name(table_name)
    storage_path = config.table_storage_path(table_name)
    try:
        writer = df.write.format("delta").mode(config.write_mode)
        if storage_path:
            writer = writer.option("path", storage_path)
        if config.catalog:
            writer.saveAsTable(qualified)
        else:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.schema_name}")
            writer.saveAsTable(qualified)
    except Exception as exc:
        raise GoldWriteError(
            f"Failed to write Gold Delta table '{qualified}' (mode={config.write_mode}): {exc}"
        ) from exc
