"""
Shared Bronze ingestion utilities: schemas, config, read/transform, Delta write, logging.

Write strategy (assessment default)
------------------------------------
- Entity Bronze tables (`bronze_customers`, `bronze_orders`, `bronze_products`):
  **overwrite** on each run. Bronze tables represent the **latest development snapshot**
  of each CSV. Trade-off: no immutable Bronze history on entity tables; use ingestion log
  for run audit. Production systems may prefer append-only Bronze with batch partitioning.
- `bronze_ingestion_log`: **append** to retain ingestion history across runs.

Type coercion note
------------------
CSV is read with explicit StructType schemas. Spark may set unparseable values to null
for typed columns (e.g. DATE) per CSV reader rules; intentional bad values in our
sample data are preserved as nulls or invalid FKs, not removed.

Nullable order foreign keys (`customer_id`, `product_id`) are stored as STRING in Bronze
so pandas-generated CSV values like `8952.0` are not lost when Spark cannot parse them
as INTEGER. Silver will parse and validate IDs.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class BronzeIngestionError(Exception):
    """Base error for Bronze ingestion failures."""


class BronzeSourceFileError(BronzeIngestionError):
    """Source CSV file is missing or not readable."""


class BronzeEmptySourceError(BronzeIngestionError):
    """Source CSV file contains no data rows."""


class BronzeSchemaError(BronzeIngestionError):
    """Source CSV does not match the expected column contract."""


class BronzeWriteError(BronzeIngestionError):
    """Failed to write Bronze Delta table or ingestion log."""


# ---------------------------------------------------------------------------
# Explicit CSV schemas (match Phase 1 generator output)
# ---------------------------------------------------------------------------

CUSTOMERS_CSV_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("customer_name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("country", StringType(), nullable=False),
        StructField("signup_date", DateType(), nullable=False),
        StructField("customer_segment", StringType(), nullable=False),
        StructField("lifetime_value", DecimalType(12, 2), nullable=False),
    ]
)

PRODUCTS_CSV_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("price", DecimalType(10, 2), nullable=False),
        StructField("cost", DecimalType(10, 2), nullable=False),
        StructField("stock_quantity", IntegerType(), nullable=False),
        StructField("reorder_level", IntegerType(), nullable=False),
    ]
)

ORDERS_CSV_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        # Nullable FKs stored as STRING in Bronze to preserve raw CSV values
        # (pandas may write integers as "8952.0"; Spark INT would null those).
        StructField("customer_id", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=False),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(10, 2), nullable=False),
        StructField("total_amount", DecimalType(12, 2), nullable=False),
        StructField("order_status", StringType(), nullable=False),
        StructField("payment_date", DateType(), nullable=True),
    ]
)

BRONZE_METADATA_COLUMNS = (
    "_ingestion_timestamp",
    "_source_file",
    "_batch_id",
)

INGESTION_LOG_SCHEMA = StructType(
    [
        StructField("entity_name", StringType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("row_count", IntegerType(), nullable=False),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
    ]
)

ENTITY_CONFIG: Dict[str, Dict[str, Any]] = {
    "customers": {
        "source_filename": "customers.csv",
        "table_name": "bronze_customers",
        "csv_schema": CUSTOMERS_CSV_SCHEMA,
        "required_columns": [f.name for f in CUSTOMERS_CSV_SCHEMA.fields],
    },
    "orders": {
        "source_filename": "orders.csv",
        "table_name": "bronze_orders",
        "csv_schema": ORDERS_CSV_SCHEMA,
        "required_columns": [f.name for f in ORDERS_CSV_SCHEMA.fields],
    },
    "products": {
        "source_filename": "products.csv",
        "table_name": "bronze_products",
        "csv_schema": PRODUCTS_CSV_SCHEMA,
        "required_columns": [f.name for f in PRODUCTS_CSV_SCHEMA.fields],
    },
}

INGESTION_LOG_TABLE = "bronze_ingestion_log"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def find_repo_root(start: Optional[Path] = None) -> Path:
    current = (start or Path(__file__).resolve()).parent
    for candidate in [current, *current.parents]:
        if (candidate / "README.md").exists() and (candidate / "src").is_dir():
            return candidate
    raise FileNotFoundError("Could not locate repository root.")


@dataclass
class BronzeConfig:
    """Runtime configuration for Bronze ingestion (env-driven, no hardcoded workspace)."""

    catalog: Optional[str]
    schema_name: str
    storage_path: Optional[str]
    input_dir: Path
    entity_write_mode: str = "overwrite"
    ingestion_log_write_mode: str = "append"

    @classmethod
    def from_env(cls, repo_root: Optional[Path] = None) -> "BronzeConfig":
        root = repo_root or find_repo_root()
        input_dir = os.environ.get("BRONZE_INPUT_DIR", "data")
        input_path = Path(input_dir)
        if not input_path.is_absolute():
            input_path = root / input_path
        return cls(
            catalog=os.environ.get("BRONZE_CATALOG") or None,
            schema_name=os.environ.get("BRONZE_SCHEMA", "ecommerce_medallion"),
            storage_path=os.environ.get("BRONZE_STORAGE_PATH") or None,
            input_dir=input_path,
            entity_write_mode=os.environ.get("BRONZE_ENTITY_WRITE_MODE", "overwrite"),
            ingestion_log_write_mode=os.environ.get(
                "BRONZE_LOG_WRITE_MODE", "append"
            ),
        )

    def qualified_table_name(self, table_name: str) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema_name}.{table_name}"
        return f"{self.schema_name}.{table_name}"

    def table_storage_path(self, table_name: str) -> Optional[str]:
        if not self.storage_path:
            return None
        return f"{self.storage_path.rstrip('/')}/bronze/{table_name}"


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def get_spark_session(app_name: str = "bronze-ingestion") -> SparkSession:
    """Return active Spark session (Databricks) or create a local session."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def generate_batch_id() -> str:
    """Unique id for one pipeline run."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"batch_{stamp}_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Validation & read
# ---------------------------------------------------------------------------


def validate_source_file(source_path: Path) -> None:
    if not source_path.exists():
        raise BronzeSourceFileError(
            f"Source file not found: {source_path}. "
            "Generate sample data or set BRONZE_INPUT_DIR."
        )
    if not source_path.is_file():
        raise BronzeSourceFileError(f"Source path is not a file: {source_path}")
    if source_path.stat().st_size == 0:
        raise BronzeEmptySourceError(f"Source file is empty: {source_path}")


def validate_required_columns(df: DataFrame, required_columns: Sequence[str]) -> None:
    actual = set(df.columns)
    missing = [col for col in required_columns if col not in actual]
    if missing:
        raise BronzeSchemaError(
            f"Missing required columns: {missing}. Found columns: {sorted(actual)}"
        )


def validate_csv_header(source_path: Path, required_columns: Sequence[str]) -> None:
    """Validate CSV header row contains all required source columns."""
    import csv

    with source_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise BronzeEmptySourceError(f"Source file has no header: {source_path}")
    missing = [col for col in required_columns if col not in header]
    if missing:
        raise BronzeSchemaError(
            f"Missing required columns in CSV header: {missing}. "
            f"Header columns: {header}"
        )


def read_csv_with_schema(
    spark: SparkSession,
    source_path: Path,
    schema: StructType,
    required_columns: Sequence[str],
) -> DataFrame:
    validate_source_file(source_path)
    validate_csv_header(source_path, required_columns)
    df = (
        spark.read.option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .option("nullValue", "")
        .schema(schema)
        .csv(source_path.resolve().as_uri())
    )
    if df.rdd.isEmpty():
        raise BronzeEmptySourceError(f"Source file has no data rows: {source_path}")
    return df


def add_bronze_metadata(
    df: DataFrame,
    source_file: str,
    batch_id: str,
    ingestion_timestamp: datetime,
) -> DataFrame:
    from pyspark.sql import functions as F

    ts_lit = ingestion_timestamp.replace(tzinfo=None)
    return (
        df.withColumn("_ingestion_timestamp", F.lit(ts_lit).cast(TimestampType()))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def prepare_bronze_dataframe(
    spark: SparkSession,
    entity_name: str,
    config: BronzeConfig,
    batch_id: str,
    ingestion_timestamp: datetime,
) -> DataFrame:
    """
    Read CSV, validate columns, add metadata. No business cleaning or DQ filtering.
    """
    if entity_name not in ENTITY_CONFIG:
        raise BronzeIngestionError(f"Unknown entity: {entity_name}")

    entity = ENTITY_CONFIG[entity_name]
    source_path = config.input_dir / entity["source_filename"]
    source_file_label = entity["source_filename"]

    df = read_csv_with_schema(
        spark, source_path, entity["csv_schema"], entity["required_columns"]
    )
    validate_required_columns(df, entity["required_columns"])
    return add_bronze_metadata(df, source_file_label, batch_id, ingestion_timestamp)


@dataclass
class IngestionResult:
    entity_name: str
    source_file: str
    row_count: int
    ingestion_timestamp: datetime
    batch_id: str
    status: str
    table_name: str


def ingest_entity_to_dataframe(
    spark: SparkSession,
    entity_name: str,
    config: BronzeConfig,
    batch_id: str,
    ingestion_timestamp: datetime,
) -> tuple[DataFrame, IngestionResult]:
    entity = ENTITY_CONFIG[entity_name]
    df = prepare_bronze_dataframe(
        spark, entity_name, config, batch_id, ingestion_timestamp
    )
    row_count = df.count()
    result = IngestionResult(
        entity_name=entity_name,
        source_file=entity["source_filename"],
        row_count=row_count,
        ingestion_timestamp=ingestion_timestamp,
        batch_id=batch_id,
        status="SUCCESS",
        table_name=entity["table_name"],
    )
    return df, result


# ---------------------------------------------------------------------------
# Delta write
# ---------------------------------------------------------------------------


def write_bronze_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    config: BronzeConfig,
    mode: Optional[str] = None,
) -> None:
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
        raise BronzeWriteError(
            f"Failed to write Delta table '{qualified}' (mode={write_mode}): {exc}"
        ) from exc


def build_ingestion_log_df(
    spark: SparkSession,
    results: List[IngestionResult],
) -> DataFrame:
    rows = [
        {
            "entity_name": r.entity_name,
            "source_file": r.source_file,
            "row_count": r.row_count,
            "ingestion_timestamp": r.ingestion_timestamp.replace(tzinfo=None),
            "batch_id": r.batch_id,
            "status": r.status,
        }
        for r in results
    ]
    return spark.createDataFrame(rows, schema=INGESTION_LOG_SCHEMA)


def write_ingestion_log(
    spark: SparkSession,
    results: List[IngestionResult],
    config: BronzeConfig,
) -> None:
    log_df = build_ingestion_log_df(spark, results)
    write_bronze_delta_table(
        spark,
        log_df,
        INGESTION_LOG_TABLE,
        config,
        mode=config.ingestion_log_write_mode,
    )


def ingest_entity(
    spark: SparkSession,
    entity_name: str,
    config: BronzeConfig,
    batch_id: str,
    ingestion_timestamp: datetime,
    write_delta: bool = True,
) -> IngestionResult:
    """Full entity ingest: read, metadata, optional Delta write."""
    df, result = ingest_entity_to_dataframe(
        spark, entity_name, config, batch_id, ingestion_timestamp
    )
    if write_delta:
        write_bronze_delta_table(spark, df, result.table_name, config)
    return result


def ingest_all_entities(
    spark: SparkSession,
    config: BronzeConfig,
    batch_id: Optional[str] = None,
    ingestion_timestamp: Optional[datetime] = None,
    write_delta: bool = True,
) -> List[IngestionResult]:
    batch_id = batch_id or generate_batch_id()
    ingestion_timestamp = ingestion_timestamp or datetime.now(timezone.utc)
    order = ["customers", "orders", "products"]
    results: List[IngestionResult] = []
    for entity_name in order:
        results.append(
            ingest_entity(
                spark,
                entity_name,
                config,
                batch_id,
                ingestion_timestamp,
                write_delta=write_delta,
            )
        )
    if write_delta:
        write_ingestion_log(spark, results, config)
    return results
