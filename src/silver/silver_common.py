"""
Shared Silver validation utilities: reason codes, FK normalization, dq_status derivation.

Silver principle: establish trusted types and explicit row-level DQ results without
deleting failed rows. Bronze STRING FK values are normalized here; raw values remain
in Bronze tables for traceability (no duplicate raw FK columns in Silver).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType

# ---------------------------------------------------------------------------
# DQ status values
# ---------------------------------------------------------------------------

DQ_STATUS_PASS = "PASS"
DQ_STATUS_FAIL = "FAIL"

# ---------------------------------------------------------------------------
# Reason codes — completeness
# ---------------------------------------------------------------------------

NULL_CUSTOMER_ID = "NULL_CUSTOMER_ID"
NULL_EMAIL = "NULL_EMAIL"
NULL_PRODUCT_ID = "NULL_PRODUCT_ID"
NULL_ORDER_ID = "NULL_ORDER_ID"

# ---------------------------------------------------------------------------
# Reason codes — type / normalization
# ---------------------------------------------------------------------------

INVALID_CUSTOMER_ID_TYPE = "INVALID_CUSTOMER_ID_TYPE"
INVALID_PRODUCT_ID_TYPE = "INVALID_PRODUCT_ID_TYPE"
INVALID_INTEGER_RANGE = "INVALID_INTEGER_RANGE"

# ---------------------------------------------------------------------------
# Reason codes — business / type rules
# ---------------------------------------------------------------------------

INVALID_QUANTITY = "INVALID_QUANTITY"
INVALID_UNIT_PRICE = "INVALID_UNIT_PRICE"
INVALID_TOTAL_AMOUNT = "INVALID_TOTAL_AMOUNT"
INVALID_CUSTOMER_SEGMENT = "INVALID_CUSTOMER_SEGMENT"
INVALID_ORDER_STATUS = "INVALID_ORDER_STATUS"
INVALID_PRICE = "INVALID_PRICE"
INVALID_COST = "INVALID_COST"
INVALID_PAYMENT_DATE = "INVALID_PAYMENT_DATE"

# ---------------------------------------------------------------------------
# Reason codes — uniqueness
# ---------------------------------------------------------------------------

DUPLICATE_CUSTOMER_ID = "DUPLICATE_CUSTOMER_ID"
DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
DUPLICATE_PRODUCT_ID = "DUPLICATE_PRODUCT_ID"

# ---------------------------------------------------------------------------
# Reason codes — referential integrity
# ---------------------------------------------------------------------------

INVALID_CUSTOMER_ID = "INVALID_CUSTOMER_ID"
INVALID_PRODUCT_ID = "INVALID_PRODUCT_ID"

ALLOWED_CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
ALLOWED_ORDER_STATUSES = ("Pending", "Completed", "Cancelled")

# Spark INT bounds
INT_MIN = -(2**31)
INT_MAX = 2**31 - 1

# Python-side FK normalization (for unit tests without Spark)
_INTEGER_STRING_PATTERN = re.compile(r"^-?\d+$")
_INTEGER_DOT_ZERO_PATTERN = re.compile(r"^-?\d+\.0+$")


def normalize_integer_string(value: str | None) -> tuple[int | None, str | None]:
    """
    Normalize a Bronze STRING FK to integer.

    Returns (normalized_value, error_reason_code).
    error_reason_code is None on success.
    NULL input returns (None, None) — completeness handles NULL/blank separately.
    """
    if value is None:
        return None, None
    stripped = value.strip()
    if stripped == "":
        return None, None
    if _INTEGER_STRING_PATTERN.fullmatch(stripped):
        parsed = int(stripped)
    elif _INTEGER_DOT_ZERO_PATTERN.fullmatch(stripped):
        parsed = int(stripped.split(".")[0])
    else:
        return None, INVALID_CUSTOMER_ID_TYPE  # caller may use product variant code

    if parsed < INT_MIN or parsed > INT_MAX:
        return None, INVALID_INTEGER_RANGE
    return parsed, None


def is_blank(column: Column) -> Column:
    """True when value is NULL or empty/whitespace after trim."""
    trimmed = F.trim(column)
    return column.isNull() | (trimmed == "")


def build_reason_array_from_rules(rules: Sequence[Tuple[Column, str]]) -> Column:
    """Build filtered array of reason codes for rules whose condition is true."""
    if not rules:
        return F.array().cast(ArrayType(StringType()))
    reason_array = F.array(
        *[F.when(cond, F.lit(code)).otherwise(F.lit(None)) for cond, code in rules]
    )
    return F.filter(reason_array, lambda x: x.isNotNull())


def merge_reason_arrays(existing: Column, new_reasons: Column) -> Column:
    """Union reason-code arrays without duplicate entries."""
    return F.array_distinct(F.array_union(existing, new_reasons))


def derive_dq_status(reasons_col: Column) -> Column:
    """PASS when no reasons; FAIL otherwise."""
    return F.when(F.size(reasons_col) > 0, F.lit(DQ_STATUS_FAIL)).otherwise(F.lit(DQ_STATUS_PASS))


def append_validation_reasons(df: DataFrame, rules: Sequence[Tuple[Column, str]]) -> DataFrame:
    """Append reason codes from rules, preserving existing reasons."""
    new_reasons = build_reason_array_from_rules(rules)
    if "dq_failure_reasons" not in df.columns:
        df = df.withColumn("dq_failure_reasons", empty_reason_array())
    return df.withColumn(
        "dq_failure_reasons",
        merge_reason_arrays(F.col("dq_failure_reasons"), new_reasons),
    ).withColumn("dq_status", derive_dq_status(F.col("dq_failure_reasons")))


def finalize_dq_columns(df: DataFrame) -> DataFrame:
    """Ensure dq_status reflects dq_failure_reasons."""
    return df.withColumn("dq_status", derive_dq_status(F.col("dq_failure_reasons")))


def empty_reason_array() -> Column:
    return F.array().cast(ArrayType(StringType()))


def trim_fk_string(column: Column) -> Column:
    return F.trim(column)


def fk_is_valid_integer_format(trimmed: Column) -> Column:
    """True for integer strings like 8952 or 8952.0 (not 12.5)."""
    return trimmed.rlike(r"^-?\d+$") | trimmed.rlike(r"^-?\d+\.0+$")


def normalize_fk_to_int(trimmed: Column, valid_format: Column) -> Column:
    """Cast valid-format trimmed strings to IntegerType; else null."""
    return F.when(
        valid_format,
        F.when(trimmed.rlike(r"^-?\d+$"), trimmed.cast(IntegerType())).otherwise(
            F.regexp_replace(trimmed, r"\.0+$", "").cast(IntegerType())
        ),
    ).otherwise(F.lit(None).cast(IntegerType()))


def normalize_order_foreign_keys(df: DataFrame) -> DataFrame:
    """
    Replace STRING Bronze FK columns with normalized INTEGER columns.

    Does not add DQ reason codes — type validation adds INVALID_*_TYPE separately.
    """
    cust_trimmed = trim_fk_string(F.col("customer_id"))
    prod_trimmed = trim_fk_string(F.col("product_id"))
    cust_valid = fk_is_valid_integer_format(cust_trimmed)
    prod_valid = fk_is_valid_integer_format(prod_trimmed)

    return (
        df.withColumn("_customer_id_raw", F.col("customer_id"))
        .withColumn("_product_id_raw", F.col("product_id"))
        .withColumn("customer_id", normalize_fk_to_int(cust_trimmed, cust_valid))
        .withColumn("product_id", normalize_fk_to_int(prod_trimmed, prod_valid))
    )


def invalid_fk_type_condition(raw_col: str) -> Column:
    """Non-blank raw FK string that is not a valid integer / integer.0 format."""
    raw = F.col(raw_col)
    trimmed = trim_fk_string(raw)
    return (~is_blank(raw)) & (~fk_is_valid_integer_format(trimmed))


def add_silver_processed_timestamp(df: DataFrame) -> DataFrame:
    ts = datetime.now(timezone.utc).replace(tzinfo=None)
    return df.withColumn("_silver_processed_at", F.lit(ts).cast("timestamp"))


def count_rows_with_reason(df: DataFrame, reason_code: str) -> int:
    return df.filter(F.array_contains(F.col("dq_failure_reasons"), reason_code)).count()
