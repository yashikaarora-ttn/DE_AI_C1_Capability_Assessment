"""
Silver type normalization, FK parsing, and business/type rule validation.

Order FK normalization runs here after completeness checks on Bronze STRING columns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.silver_common import (
    ALLOWED_CUSTOMER_SEGMENTS,
    ALLOWED_ORDER_STATUSES,
    INVALID_COST,
    INVALID_CUSTOMER_ID_TYPE,
    INVALID_CUSTOMER_SEGMENT,
    INVALID_ORDER_STATUS,
    INVALID_PAYMENT_DATE,
    INVALID_PRICE,
    INVALID_PRODUCT_ID_TYPE,
    INVALID_QUANTITY,
    INVALID_TOTAL_AMOUNT,
    INVALID_UNIT_PRICE,
    build_reason_array_from_rules,
    derive_dq_status,
    empty_reason_array,
    invalid_fk_type_condition,
    merge_reason_arrays,
    normalize_order_foreign_keys,
    trim_fk_string,
)

DECIMAL_TOLERANCE = 0.01


def _append_type_reasons(df: DataFrame, rules: list) -> DataFrame:
    new_reasons = build_reason_array_from_rules(rules)
    if "dq_failure_reasons" not in df.columns:
        df = df.withColumn("dq_failure_reasons", empty_reason_array())
    return df.withColumn(
        "dq_failure_reasons",
        merge_reason_arrays(F.col("dq_failure_reasons"), new_reasons),
    ).withColumn("dq_status", derive_dq_status(F.col("dq_failure_reasons")))


def validate_customers_type_rules(df: DataFrame) -> DataFrame:
    allowed = list(ALLOWED_CUSTOMER_SEGMENTS)
    rules = [
        (
            F.col("customer_segment").isNotNull()
            & ~F.col("customer_segment").isin(allowed),
            INVALID_CUSTOMER_SEGMENT,
        ),
        (F.col("lifetime_value").isNull() | (F.col("lifetime_value") < 0), "INVALID_LIFETIME_VALUE"),
    ]
    return _append_type_reasons(df, rules)


def validate_products_type_rules(df: DataFrame) -> DataFrame:
    rules = [
        (F.col("price").isNull() | (F.col("price") <= 0), INVALID_PRICE),
        (F.col("cost").isNull() | (F.col("cost") < 0), INVALID_COST),
        (F.col("stock_quantity").isNull() | (F.col("stock_quantity") < 0), "INVALID_STOCK_QUANTITY"),
        (F.col("reorder_level").isNull() | (F.col("reorder_level") < 0), "INVALID_REORDER_LEVEL"),
    ]
    return _append_type_reasons(df, rules)


def validate_orders_type_rules(df: DataFrame) -> DataFrame:
    """Normalize FKs then apply type and business-rule checks."""
    df = normalize_order_foreign_keys(df)

    fk_rules = [
        (invalid_fk_type_condition("_customer_id_raw"), INVALID_CUSTOMER_ID_TYPE),
        (invalid_fk_type_condition("_product_id_raw"), INVALID_PRODUCT_ID_TYPE),
    ]
    df = _append_type_reasons(df, fk_rules)

    expected_total = F.col("quantity").cast("double") * F.col("unit_price").cast("double")
    actual_total = F.col("total_amount").cast("double")
    total_mismatch = F.abs(actual_total - expected_total) > DECIMAL_TOLERANCE

    business_rules = [
        (F.col("quantity").isNull() | (F.col("quantity") <= 0), INVALID_QUANTITY),
        (F.col("unit_price").isNull() | (F.col("unit_price") < 0), INVALID_UNIT_PRICE),
        (total_mismatch, INVALID_TOTAL_AMOUNT),
        (~F.col("order_status").isin(list(ALLOWED_ORDER_STATUSES)), INVALID_ORDER_STATUS),
        (
            F.col("payment_date").isNotNull()
            & F.col("order_date").isNotNull()
            & (F.col("payment_date") < F.col("order_date")),
            INVALID_PAYMENT_DATE,
        ),
    ]
    return _append_type_reasons(df, business_rules)


def apply_type_validation(entity: str, df: DataFrame) -> DataFrame:
    validators = {
        "customers": validate_customers_type_rules,
        "products": validate_products_type_rules,
        "orders": validate_orders_type_rules,
    }
    if entity not in validators:
        raise ValueError(f"Unknown entity for type validation: {entity}")
    return validators[entity](df)


def drop_order_fk_raw_columns(df: DataFrame) -> DataFrame:
    """Remove temporary raw FK columns after validation."""
    cols = [c for c in ("_customer_id_raw", "_product_id_raw") if c in df.columns]
    if cols:
        return df.drop(*cols)
    return df
