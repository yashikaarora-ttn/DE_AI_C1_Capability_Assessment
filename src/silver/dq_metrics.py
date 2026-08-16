"""
Build Silver DQ metrics DataFrames from validated entity DataFrames.

Produces two metric types per entity:
- RULE: per reason-code failure counts (zero-failure rules still reported)
- OVERALL: row-level PASS/FAIL totals (not summed from reason codes)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver.silver_common import DQ_STATUS_FAIL, DQ_STATUS_PASS

METRIC_TYPE_RULE = "RULE"
METRIC_TYPE_OVERALL = "OVERALL"

DQ_METRICS_SCHEMA = StructType(
    [
        StructField("entity_name", StringType(), nullable=False),
        StructField("metric_type", StringType(), nullable=False),
        StructField("reason_code", StringType(), nullable=True),
        StructField("rule_id", StringType(), nullable=True),
        StructField("total_records", IntegerType(), nullable=False),
        StructField("failed_count", IntegerType(), nullable=False),
        StructField("passed_count", IntegerType(), nullable=False),
        StructField("failed_percentage", DoubleType(), nullable=False),
        StructField("passed_percentage", DoubleType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("metric_timestamp", TimestampType(), nullable=False),
    ]
)

# Reason codes reported per entity (includes rules with zero failures on clean data).
ENTITY_REASON_CODES: Dict[str, List[str]] = {
    "customers": [
        "NULL_CUSTOMER_ID",
        "NULL_EMAIL",
        "DUPLICATE_CUSTOMER_ID",
        "INVALID_CUSTOMER_SEGMENT",
        "INVALID_LIFETIME_VALUE",
    ],
    "products": [
        "NULL_PRODUCT_ID",
        "DUPLICATE_PRODUCT_ID",
        "INVALID_PRICE",
        "INVALID_COST",
        "INVALID_STOCK_QUANTITY",
        "INVALID_REORDER_LEVEL",
    ],
    "orders": [
        "NULL_ORDER_ID",
        "NULL_CUSTOMER_ID",
        "NULL_PRODUCT_ID",
        "INVALID_CUSTOMER_ID_TYPE",
        "INVALID_PRODUCT_ID_TYPE",
        "INVALID_QUANTITY",
        "INVALID_UNIT_PRICE",
        "INVALID_TOTAL_AMOUNT",
        "INVALID_ORDER_STATUS",
        "INVALID_PAYMENT_DATE",
        "DUPLICATE_ORDER_ID",
        "INVALID_CUSTOMER_ID",
        "INVALID_PRODUCT_ID",
    ],
}

RULE_ID_MAP: Dict[str, str] = {
    "NULL_EMAIL": "COMP-CUST-02",
    "NULL_CUSTOMER_ID": "COMP-ORD-02",
    "NULL_PRODUCT_ID": "COMP-ORD-03",
    "NULL_ORDER_ID": "COMP-ORD-01",
    "DUPLICATE_CUSTOMER_ID": "UNIQ-CUST-01",
    "DUPLICATE_ORDER_ID": "UNIQ-ORD-01",
    "DUPLICATE_PRODUCT_ID": "UNIQ-PROD-01",
    "INVALID_CUSTOMER_ID": "RI-ORD-01",
    "INVALID_PRODUCT_ID": "RI-ORD-02",
}


def _pct(part: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(part * 100.0 / total, 4)


def _rule_metric_row(
    entity_name: str,
    reason_code: str,
    total_records: int,
    failed_count: int,
    batch_id: str,
    metric_timestamp: datetime,
) -> dict:
    passed_count = total_records - failed_count
    return {
        "entity_name": entity_name,
        "metric_type": METRIC_TYPE_RULE,
        "reason_code": reason_code,
        "rule_id": RULE_ID_MAP.get(reason_code),
        "total_records": total_records,
        "failed_count": failed_count,
        "passed_count": passed_count,
        "failed_percentage": _pct(failed_count, total_records),
        "passed_percentage": _pct(passed_count, total_records),
        "batch_id": batch_id,
        "metric_timestamp": metric_timestamp.replace(tzinfo=None),
    }


def _overall_metric_row(
    entity_name: str,
    total_records: int,
    pass_count: int,
    fail_count: int,
    batch_id: str,
    metric_timestamp: datetime,
) -> dict:
    return {
        "entity_name": entity_name,
        "metric_type": METRIC_TYPE_OVERALL,
        "reason_code": None,
        "rule_id": None,
        "total_records": total_records,
        "failed_count": fail_count,
        "passed_count": pass_count,
        "failed_percentage": _pct(fail_count, total_records),
        "passed_percentage": _pct(pass_count, total_records),
        "batch_id": batch_id,
        "metric_timestamp": metric_timestamp.replace(tzinfo=None),
    }


def build_entity_metrics(
    df: DataFrame,
    entity_name: str,
    batch_id: str,
    metric_timestamp: datetime,
    reason_codes: Sequence[str] | None = None,
) -> List[dict]:
    """Build RULE and OVERALL metric rows for one Silver entity DataFrame."""
    codes = list(reason_codes or ENTITY_REASON_CODES.get(entity_name, []))
    total_records = df.count()
    pass_count = df.filter(F.col("dq_status") == DQ_STATUS_PASS).count()
    fail_count = df.filter(F.col("dq_status") == DQ_STATUS_FAIL).count()

    rows: List[dict] = []
    for code in codes:
        failed = df.filter(F.array_contains(F.col("dq_failure_reasons"), code)).count()
        rows.append(
            _rule_metric_row(
                entity_name, code, total_records, failed, batch_id, metric_timestamp
            )
        )
    rows.append(
        _overall_metric_row(
            entity_name, total_records, pass_count, fail_count, batch_id, metric_timestamp
        )
    )
    return rows


def build_dq_metrics_df(
    spark: SparkSession,
    silver_dfs: Dict[str, DataFrame],
    batch_id: str,
    metric_timestamp: datetime | None = None,
) -> DataFrame:
    """Build combined DQ metrics DataFrame for all Silver entities."""
    metric_timestamp = metric_timestamp or datetime.now(timezone.utc)
    all_rows: List[dict] = []
    for entity_name, df in silver_dfs.items():
        all_rows.extend(
            build_entity_metrics(df, entity_name, batch_id, metric_timestamp)
        )
    return spark.createDataFrame(all_rows, schema=DQ_METRICS_SCHEMA)


def count_reason_failures(metrics_df: DataFrame, entity: str, reason_code: str) -> int:
    row = metrics_df.filter(
        (F.col("entity_name") == entity)
        & (F.col("metric_type") == METRIC_TYPE_RULE)
        & (F.col("reason_code") == reason_code)
    ).collect()
    return row[0]["failed_count"] if row else 0


def get_overall_metrics(metrics_df: DataFrame, entity: str) -> dict:
    rows = metrics_df.filter(
        (F.col("entity_name") == entity) & (F.col("metric_type") == METRIC_TYPE_OVERALL)
    ).collect()
    if not rows:
        raise ValueError(f"No OVERALL metrics for entity {entity}")
    return rows[0].asDict()
