"""
Silver completeness validation for customers, products, and orders.

Blank strings are treated as incomplete (same as NULL) for required string fields.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.silver_common import (
    NULL_CUSTOMER_ID,
    NULL_EMAIL,
    NULL_ORDER_ID,
    NULL_PRODUCT_ID,
    build_reason_array_from_rules,
    derive_dq_status,
    empty_reason_array,
    is_blank,
    merge_reason_arrays,
)


def _with_completeness_reasons(df: DataFrame, rules: list) -> DataFrame:
    new_reasons = build_reason_array_from_rules(rules)
    if "dq_failure_reasons" not in df.columns:
        df = df.withColumn("dq_failure_reasons", empty_reason_array())
    return df.withColumn(
        "dq_failure_reasons",
        merge_reason_arrays(F.col("dq_failure_reasons"), new_reasons),
    ).withColumn("dq_status", derive_dq_status(F.col("dq_failure_reasons")))


def validate_customers_completeness(df: DataFrame) -> DataFrame:
    """Required: customer_id, email (NULL or blank email fails)."""
    rules = [
        (F.col("customer_id").isNull(), NULL_CUSTOMER_ID),
        (is_blank(F.col("email")), NULL_EMAIL),
    ]
    return _with_completeness_reasons(df, rules)


def validate_products_completeness(df: DataFrame) -> DataFrame:
    """Required identifiers and critical attributes for products."""
    rules = [
        (F.col("product_id").isNull(), NULL_PRODUCT_ID),
    ]
    return _with_completeness_reasons(df, rules)


def validate_orders_completeness(df: DataFrame) -> DataFrame:
    """
    Required: order_id, customer_id, product_id.

    Runs on Bronze STRING FK columns before integer normalization.
    """
    rules = [
        (F.col("order_id").isNull(), NULL_ORDER_ID),
        (is_blank(F.col("customer_id")), NULL_CUSTOMER_ID),
        (is_blank(F.col("product_id")), NULL_PRODUCT_ID),
    ]
    return _with_completeness_reasons(df, rules)


def apply_completeness_validation(entity: str, df: DataFrame) -> DataFrame:
    validators = {
        "customers": validate_customers_completeness,
        "products": validate_products_completeness,
        "orders": validate_orders_completeness,
    }
    if entity not in validators:
        raise ValueError(f"Unknown entity for completeness validation: {entity}")
    return validators[entity](df)
