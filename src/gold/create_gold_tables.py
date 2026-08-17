"""
Create Gold Delta tables: trusted Silver filtering → business aggregations.

Orchestration:
  Silver input → trusted filtering → Gold aggregations → optional Delta writes (overwrite)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, SparkSession

from bronze.bronze_common import get_spark_session
from gold.gold_common import (
    build_customer_segmentation,
    build_daily_weekly_trends,
    build_revenue_by_customer,
    build_sales_by_product,
)
from gold.gold_config import GOLD_TABLES, GoldConfig, GoldWriteError, write_gold_delta_table
from silver.silver_config import SILVER_ENTITY_TABLES, SilverConfig


@dataclass
class GoldPipelineResult:
    """Outputs from a Gold pipeline run."""

    gold_dfs: Dict[str, DataFrame]
    written_tables: List[str]


def build_all_gold_dfs(
    silver_dfs: Dict[str, DataFrame],
    config: GoldConfig,
) -> Dict[str, DataFrame]:
    """Build all Gold aggregation DataFrames from in-memory Silver outputs."""
    orders = silver_dfs["orders"]
    customers = silver_dfs["customers"]
    products = silver_dfs["products"]
    return {
        "sales_by_product": build_sales_by_product(orders, products, customers),
        "revenue_by_customer": build_revenue_by_customer(orders, customers, products),
        "daily_weekly_trends": build_daily_weekly_trends(orders, customers, products),
        "customer_segmentation": build_customer_segmentation(
            orders,
            customers,
            products,
            config.high_value_threshold,
        ),
    }


def load_silver_dataframes(
    spark: SparkSession,
    silver_config: SilverConfig,
) -> Dict[str, DataFrame]:
    """Load Silver entity tables from Delta."""
    dfs: Dict[str, DataFrame] = {}
    for entity, table_name in SILVER_ENTITY_TABLES.items():
        qualified = silver_config.qualified_table_name(table_name)
        dfs[entity] = spark.table(qualified)
    return dfs


def run_gold_pipeline(
    spark: SparkSession,
    silver_dfs: Dict[str, DataFrame],
    config: GoldConfig,
    write_delta: bool = True,
) -> GoldPipelineResult:
    """Run Gold aggregations and optionally write snapshot tables to Delta."""
    gold_dfs = build_all_gold_dfs(silver_dfs, config)
    written_tables: List[str] = []
    if write_delta:
        for key, table_name in GOLD_TABLES.items():
            write_gold_delta_table(spark, gold_dfs[key], table_name, config)
            written_tables.append(table_name)
    return GoldPipelineResult(gold_dfs=gold_dfs, written_tables=written_tables)


def run_gold_from_silver_delta(
    spark: SparkSession,
    silver_config: Optional[SilverConfig] = None,
    gold_config: Optional[GoldConfig] = None,
    write_delta: bool = True,
) -> GoldPipelineResult:
    """Load Silver from Delta, build Gold, optionally write Gold Delta tables."""
    silver_config = silver_config or SilverConfig.from_env()
    gold_config = gold_config or GoldConfig.from_env()
    silver_dfs = load_silver_dataframes(spark, silver_config)
    return run_gold_pipeline(spark, silver_dfs, gold_config, write_delta=write_delta)


def main() -> int:
    spark = get_spark_session("gold-create-tables")
    gold_config = GoldConfig.from_env()
    try:
        result = run_gold_from_silver_delta(spark, write_delta=True)
    except GoldWriteError as exc:
        print(f"Gold write failed: {exc}", file=sys.stderr)
        return 1

    print("Gold pipeline complete.")
    for key, table_name in GOLD_TABLES.items():
        print(f"  {table_name}: {result.gold_dfs[key].count()} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
