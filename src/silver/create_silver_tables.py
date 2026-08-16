"""
Create Silver Delta tables: full validation pipeline, entity writes, and DQ metrics.

Orchestration:
  Bronze input → completeness → type/business → uniqueness → RI → Silver writes → metrics
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, SparkSession

from bronze.bronze_common import (
    BronzeConfig,
    generate_batch_id,
    get_spark_session,
    prepare_bronze_dataframe,
)
from silver.dq_metrics import build_dq_metrics_df, DQ_METRICS_SCHEMA
from silver.silver_config import (
    BRONZE_TABLE_NAMES,
    DQ_METRICS_TABLE,
    SILVER_ENTITY_TABLES,
    SilverConfig,
    SilverWriteError,
    write_silver_delta_table,
    write_silver_metrics,
)
from silver.silver_foundation import apply_silver_all


@dataclass
class SilverPipelineResult:
    """Outputs from a Silver pipeline run (in-memory + optional Delta writes)."""

    batch_id: str
    metric_timestamp: datetime
    silver_dfs: Dict[str, DataFrame]
    metrics_df: DataFrame
    written_tables: List[str]


def load_bronze_dataframes(
    spark: SparkSession,
    bronze_config: BronzeConfig,
    silver_config: SilverConfig,
    batch_id: str,
    ingestion_timestamp: datetime,
) -> Dict[str, DataFrame]:
    """Load Bronze DataFrames from Delta tables or CSV preparation."""
    entities = ("customers", "products", "orders")
    if silver_config.read_bronze_from_delta:
        dfs: Dict[str, DataFrame] = {}
        for entity in entities:
            table = bronze_config.qualified_table_name(BRONZE_TABLE_NAMES[entity])
            dfs[entity] = spark.table(table)
        return dfs

    return {
        entity: prepare_bronze_dataframe(
            spark, entity, bronze_config, batch_id, ingestion_timestamp
        )
        for entity in entities
    }


def run_silver_pipeline(
    spark: SparkSession,
    bronze_config: BronzeConfig,
    silver_config: SilverConfig,
    batch_id: Optional[str] = None,
    ingestion_timestamp: Optional[datetime] = None,
    metric_timestamp: Optional[datetime] = None,
    write_delta: bool = True,
) -> SilverPipelineResult:
    """
    Run full Silver validation and optionally write entity tables + metrics to Delta.
    """
    batch_id = batch_id or generate_batch_id()
    ingestion_timestamp = ingestion_timestamp or datetime.now(timezone.utc)
    metric_timestamp = metric_timestamp or datetime.now(timezone.utc)

    bronze_dfs = load_bronze_dataframes(
        spark, bronze_config, silver_config, batch_id, ingestion_timestamp
    )
    silver_dfs = apply_silver_all(bronze_dfs)
    metrics_df = build_dq_metrics_df(spark, silver_dfs, batch_id, metric_timestamp)

    written_tables: List[str] = []
    if write_delta:
        for entity, table_name in SILVER_ENTITY_TABLES.items():
            write_silver_delta_table(spark, silver_dfs[entity], table_name, silver_config)
            written_tables.append(table_name)
        write_silver_metrics(spark, metrics_df, silver_config)
        written_tables.append(DQ_METRICS_TABLE)

    return SilverPipelineResult(
        batch_id=batch_id,
        metric_timestamp=metric_timestamp,
        silver_dfs=silver_dfs,
        metrics_df=metrics_df,
        written_tables=written_tables,
    )


def main() -> int:
    spark = get_spark_session("silver-create-tables")
    bronze_config = BronzeConfig.from_env()
    silver_config = SilverConfig.from_env()
    batch_id = generate_batch_id()

    try:
        result = run_silver_pipeline(
            spark,
            bronze_config,
            silver_config,
            batch_id=batch_id,
            write_delta=True,
        )
    except SilverWriteError as exc:
        print(f"Silver write failed: {exc}", file=sys.stderr)
        return 1

    print(f"Silver pipeline complete. batch_id={result.batch_id}")
    for entity, df in result.silver_dfs.items():
        print(f"  {SILVER_ENTITY_TABLES[entity]}: {df.count()} rows")
    print(f"  {DQ_METRICS_TABLE}: {result.metrics_df.count()} metric rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
