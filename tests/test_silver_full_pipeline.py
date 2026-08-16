"""Full Silver pipeline validation on generated dataset."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from bronze.bronze_common import BronzeConfig, prepare_bronze_dataframe  # noqa: E402
from data_generation.generate_sample_data import (  # noqa: E402
    DUPLICATE_CUSTOMER_ID_ROWS,
    DUPLICATE_ORDER_ID_ROWS,
    NULL_EMAIL_COUNT,
    NULL_ORDER_CUSTOMER_ID_COUNT,
    NULL_ORDER_PRODUCT_ID_COUNT,
    INVALID_ORDER_CUSTOMER_ID_COUNT,
    INVALID_ORDER_PRODUCT_ID_COUNT,
    NUM_CUSTOMERS,
    NUM_ORDERS,
    NUM_PRODUCTS,
    generate_all,
)
from silver.silver_common import (  # noqa: E402
    DQ_STATUS_FAIL,
    DQ_STATUS_PASS,
    DUPLICATE_CUSTOMER_ID,
    DUPLICATE_ORDER_ID,
    DUPLICATE_PRODUCT_ID,
    INVALID_CUSTOMER_ID,
    INVALID_PRODUCT_ID,
    NULL_CUSTOMER_ID,
    NULL_EMAIL,
    NULL_PRODUCT_ID,
)
from silver.silver_foundation import apply_silver_all  # noqa: E402

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
        schema_name="silver_full",
        storage_path=None,
        input_dir=sample_data_dir,
    )


def test_full_silver_pipeline_counts(spark: SparkSession, bronze_config: BronzeConfig):
    batch_id = "full_silver"
    ts = datetime(2026, 8, 16, tzinfo=timezone.utc)
    bronze_dfs = {
        entity: prepare_bronze_dataframe(spark, entity, bronze_config, batch_id, ts)
        for entity in ("customers", "products", "orders")
    }
    silver = apply_silver_all(bronze_dfs)

    # Row counts unchanged
    assert silver["customers"].count() == NUM_CUSTOMERS
    assert silver["products"].count() == NUM_PRODUCTS
    assert silver["orders"].count() == NUM_ORDERS

    customers = silver["customers"]
    assert customers.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_EMAIL)).count() == NULL_EMAIL_COUNT
    assert customers.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_CUSTOMER_ID)
    ).count() == DUPLICATE_CUSTOMER_ID_ROWS

    products = silver["products"]
    assert products.filter(
        F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_PRODUCT_ID)
    ).count() == 0

    orders = silver["orders"]
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_CUSTOMER_ID)).count() == NULL_ORDER_CUSTOMER_ID_COUNT
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), NULL_PRODUCT_ID)).count() == NULL_ORDER_PRODUCT_ID_COUNT
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), INVALID_CUSTOMER_ID)).count() == INVALID_ORDER_CUSTOMER_ID_COUNT
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), INVALID_PRODUCT_ID)).count() == INVALID_ORDER_PRODUCT_ID_COUNT
    assert orders.filter(F.array_contains(F.col("dq_failure_reasons"), DUPLICATE_ORDER_ID)).count() == DUPLICATE_ORDER_ID_ROWS

    # PASS/FAIL from dq_status (not sum of reason codes)
    cust_pass = customers.filter(F.col("dq_status") == DQ_STATUS_PASS).count()
    cust_fail = customers.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()
    assert cust_pass + cust_fail == NUM_CUSTOMERS

    prod_pass = products.filter(F.col("dq_status") == DQ_STATUS_PASS).count()
    prod_fail = products.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()
    assert prod_pass + prod_fail == NUM_PRODUCTS

    ord_pass = orders.filter(F.col("dq_status") == DQ_STATUS_PASS).count()
    ord_fail = orders.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()
    assert ord_pass + ord_fail == NUM_ORDERS

    assert cust_fail == NULL_EMAIL_COUNT + DUPLICATE_CUSTOMER_ID_ROWS
    assert prod_fail == 0
    assert ord_fail == (
        NULL_ORDER_CUSTOMER_ID_COUNT
        + NULL_ORDER_PRODUCT_ID_COUNT
        + INVALID_ORDER_CUSTOMER_ID_COUNT
        + INVALID_ORDER_PRODUCT_ID_COUNT
        + DUPLICATE_ORDER_ID_ROWS
    )
