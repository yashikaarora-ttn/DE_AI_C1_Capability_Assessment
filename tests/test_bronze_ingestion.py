"""Tests for Bronze CSV ingestion (local Spark; Delta write validated on Databricks)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from bronze.bronze_common import (  # noqa: E402
    BRONZE_METADATA_COLUMNS,
    CUSTOMERS_CSV_SCHEMA,
    ENTITY_CONFIG,
    ORDERS_CSV_SCHEMA,
    PRODUCTS_CSV_SCHEMA,
    BronzeConfig,
    BronzeEmptySourceError,
    BronzeSchemaError,
    BronzeSourceFileError,
    build_ingestion_log_df,
    ingest_all_entities,
    add_bronze_metadata,
    prepare_bronze_dataframe,
    read_csv_with_schema,
    validate_required_columns,
    validate_source_file,
)
from data_generation.generate_sample_data import (  # noqa: E402
    DUPLICATE_CUSTOMER_ID_ROWS,
    DUPLICATE_ORDER_ID_ROWS,
    NULL_EMAIL_COUNT,
    NULL_ORDER_CUSTOMER_ID_COUNT,
    NULL_ORDER_PRODUCT_ID_COUNT,
    INVALID_ORDER_CUSTOMER_ID_COUNT,
    INVALID_ORDER_PRODUCT_ID_COUNT,
    INVALID_CUSTOMER_ID_START,
    INVALID_PRODUCT_ID_START,
    NUM_CUSTOMERS,
    NUM_ORDERS,
    NUM_PRODUCTS,
    count_null_emails,
    generate_all,
)

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import col  # noqa: E402


@pytest.fixture
def sample_data_dir(tmp_path) -> Path:
    generate_all(seed=42, output_dir=str(tmp_path))
    return tmp_path


@pytest.fixture
def bronze_config(sample_data_dir: Path) -> BronzeConfig:
    return BronzeConfig(
        catalog=None,
        schema_name="bronze_test",
        storage_path=None,
        input_dir=sample_data_dir,
    )


@pytest.fixture
def batch_context() -> tuple[str, datetime]:
    return "batch_test_001", datetime(2026, 8, 16, 12, 0, 0, tzinfo=timezone.utc)


def test_explicit_schemas_contain_expected_fields_and_types():
    customer_fields = {f.name: f.dataType.simpleString() for f in CUSTOMERS_CSV_SCHEMA.fields}
    assert customer_fields["customer_id"] == "int"
    assert customer_fields["email"] == "string"  # nullable in schema
    assert customer_fields["signup_date"] == "date"
    assert customer_fields["lifetime_value"] == "decimal(12,2)"

    order_fields = {f.name: f.dataType.simpleString() for f in ORDERS_CSV_SCHEMA.fields}
    assert order_fields["customer_id"] == "string"  # nullable FK — raw CSV fidelity
    assert order_fields["product_id"] == "string"
    assert order_fields["payment_date"] == "date"
    assert order_fields["unit_price"] == "decimal(10,2)"

    product_fields = {f.name: f.dataType.simpleString() for f in PRODUCTS_CSV_SCHEMA.fields}
    assert product_fields["price"] == "decimal(10,2)"
    assert product_fields["stock_quantity"] == "int"


def test_bronze_preserves_row_counts(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    for entity_name, expected_rows in (
        ("customers", NUM_CUSTOMERS),
        ("products", NUM_PRODUCTS),
        ("orders", NUM_ORDERS),
    ):
        df = prepare_bronze_dataframe(
            spark, entity_name, bronze_config, batch_id, ingestion_ts
        )
        assert df.count() == expected_rows


def test_metadata_columns_are_added(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    df = prepare_bronze_dataframe(
        spark, "customers", bronze_config, batch_id, ingestion_ts
    )
    for col in BRONZE_METADATA_COLUMNS:
        assert col in df.columns
    row = df.select(*BRONZE_METADATA_COLUMNS).first()
    assert row["_batch_id"] == batch_id
    assert row["_source_file"] == ENTITY_CONFIG["customers"]["source_filename"]
    assert row["_ingestion_timestamp"] is not None


def test_intentional_nulls_preserved(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    customers = prepare_bronze_dataframe(
        spark, "customers", bronze_config, batch_id, ingestion_ts
    )
    orders = prepare_bronze_dataframe(
        spark, "orders", bronze_config, batch_id, ingestion_ts
    )

    assert customers.filter("email IS NULL").count() == NULL_EMAIL_COUNT
    assert orders.filter("customer_id IS NULL").count() == NULL_ORDER_CUSTOMER_ID_COUNT
    assert orders.filter("product_id IS NULL").count() == NULL_ORDER_PRODUCT_ID_COUNT


def test_intentional_duplicate_ids_preserved(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    customers = prepare_bronze_dataframe(
        spark, "customers", bronze_config, batch_id, ingestion_ts
    )
    orders = prepare_bronze_dataframe(
        spark, "orders", bronze_config, batch_id, ingestion_ts
    )

    def duplicate_row_count(df, key: str) -> int:
        dup_keys = df.groupBy(key).count().filter("count > 1").select(key)
        return df.join(dup_keys, key, "inner").count()

    assert duplicate_row_count(customers, "customer_id") == DUPLICATE_CUSTOMER_ID_ROWS
    assert duplicate_row_count(orders, "order_id") == DUPLICATE_ORDER_ID_ROWS


def test_missing_required_columns_raises(spark: SparkSession, tmp_path):
    bad_csv = tmp_path / "customers.csv"
    bad_csv.write_text("customer_id,customer_name\n1,Alice\n")
    config = BronzeConfig(
        catalog=None,
        schema_name="bronze_test",
        storage_path=None,
        input_dir=tmp_path,
    )
    with pytest.raises(BronzeSchemaError, match="Missing required columns"):
        prepare_bronze_dataframe(
            spark,
            "customers",
            config,
            "batch_x",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_missing_source_file_raises(spark: SparkSession, tmp_path):
    config = BronzeConfig(
        catalog=None,
        schema_name="bronze_test",
        storage_path=None,
        input_dir=tmp_path,
    )
    with pytest.raises(BronzeSourceFileError, match="not found"):
        prepare_bronze_dataframe(
            spark,
            "customers",
            config,
            "batch_x",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_batch_id_and_source_file_populated(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    df = prepare_bronze_dataframe(
        spark, "orders", bronze_config, batch_id, ingestion_ts
    )
    distinct_batch = df.select("_batch_id").distinct().count()
    distinct_source = df.select("_source_file").distinct().count()
    assert distinct_batch == 1
    assert distinct_source == 1
    assert df.first()["_batch_id"] == batch_id
    assert df.first()["_source_file"] == "orders.csv"


def test_bronze_does_not_apply_silver_cleaning(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    df = prepare_bronze_dataframe(
        spark, "orders", bronze_config, batch_id, ingestion_ts
    )
    assert "is_valid" not in df.columns
    assert "dq_failure_reasons" not in df.columns
    assert df.count() == NUM_ORDERS


def test_validate_required_columns_helper(spark: SparkSession):
    df = spark.createDataFrame([(1, "a")], ["customer_id", "customer_name"])
    with pytest.raises(BronzeSchemaError):
        validate_required_columns(df, ["customer_id", "email"])


def test_empty_source_file_raises(tmp_path):
    empty = tmp_path / "customers.csv"
    empty.write_text("")
    with pytest.raises(BronzeEmptySourceError):
        validate_source_file(empty)


def test_header_only_csv_raises_empty_error(spark: SparkSession, tmp_path):
    header_only = tmp_path / "customers.csv"
    header_only.write_text(
        "customer_id,customer_name,email,country,signup_date,customer_segment,lifetime_value\n"
    )
    config = BronzeConfig(
        catalog=None,
        schema_name="bronze_test",
        storage_path=None,
        input_dir=tmp_path,
    )
    with pytest.raises(BronzeEmptySourceError, match="no data rows"):
        prepare_bronze_dataframe(
            spark,
            "customers",
            config,
            "batch_x",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_ingest_all_entities_shares_batch_id_and_timestamp(
    spark: SparkSession, bronze_config: BronzeConfig
):
    shared_batch = "batch_pipeline_run_001"
    shared_ts = datetime(2026, 8, 16, 14, 30, 0, tzinfo=timezone.utc)
    results = ingest_all_entities(
        spark,
        bronze_config,
        batch_id=shared_batch,
        ingestion_timestamp=shared_ts,
        write_delta=False,
    )
    assert len(results) == 3
    assert {r.entity_name for r in results} == {"customers", "orders", "products"}
    assert all(r.batch_id == shared_batch for r in results)
    assert all(r.ingestion_timestamp == shared_ts for r in results)
    assert all(r.status == "SUCCESS" for r in results)


def test_ingestion_log_reflects_entity_row_counts(
    spark: SparkSession, bronze_config: BronzeConfig
):
    results = ingest_all_entities(
        spark, bronze_config, batch_id="batch_log_test", write_delta=False
    )
    log_df = build_ingestion_log_df(spark, results)
    rows = {r.entity_name: r.row_count for r in results}
    logged = {
        row.entity_name: row.row_count
        for row in log_df.collect()
    }
    assert logged == rows
    assert logged["customers"] == NUM_CUSTOMERS
    assert logged["orders"] == NUM_ORDERS
    assert logged["products"] == NUM_PRODUCTS


def test_invalid_foreign_key_strings_preserved_for_silver(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    batch_id, ingestion_ts = batch_context
    orders = prepare_bronze_dataframe(
        spark, "orders", bronze_config, batch_id, ingestion_ts
    )
    # Match generator IDs 90001–90050 and 9001–9030; allow optional ".0" from pandas CSV.
    invalid_customer_values = {
        str(INVALID_CUSTOMER_ID_START + i)
        for i in range(INVALID_ORDER_CUSTOMER_ID_COUNT)
    } | {
        f"{INVALID_CUSTOMER_ID_START + i}.0"
        for i in range(INVALID_ORDER_CUSTOMER_ID_COUNT)
    }
    invalid_product_values = {
        str(INVALID_PRODUCT_ID_START + i)
        for i in range(INVALID_ORDER_PRODUCT_ID_COUNT)
    } | {
        f"{INVALID_PRODUCT_ID_START + i}.0"
        for i in range(INVALID_ORDER_PRODUCT_ID_COUNT)
    }
    invalid_customer = orders.filter(col("customer_id").isin(list(invalid_customer_values))).count()
    invalid_product = orders.filter(col("product_id").isin(list(invalid_product_values))).count()
    assert invalid_customer == INVALID_ORDER_CUSTOMER_ID_COUNT
    assert invalid_product == INVALID_ORDER_PRODUCT_ID_COUNT


def test_non_null_fk_values_preserved_as_raw_strings(
    spark: SparkSession, bronze_config: BronzeConfig, batch_context
):
    """Bronze keeps pandas float-formatted IDs (e.g. 8952.0) as strings for Silver to parse."""
    batch_id, ingestion_ts = batch_context
    orders = prepare_bronze_dataframe(
        spark, "orders", bronze_config, batch_id, ingestion_ts
    )
    sample = (
        orders.filter(col("customer_id").isNotNull())
        .select("customer_id")
        .limit(1)
        .collect()[0][0]
    )
    assert sample is not None
    assert isinstance(sample, str)
    assert sample.replace(".", "", 1).isdigit()


def test_add_bronze_metadata_columns(spark: SparkSession):
    df = spark.createDataFrame([(1, "Alice")], ["customer_id", "customer_name"])
    enriched = add_bronze_metadata(
        df,
        "customers.csv",
        "batch_meta",
        datetime(2026, 8, 16, 10, 0, 0, tzinfo=timezone.utc),
    )
    assert all(c in enriched.columns for c in BRONZE_METADATA_COLUMNS)
