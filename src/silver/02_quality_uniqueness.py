"""
Silver uniqueness validation — primary keys must be unique within each entity.

All rows participating in a duplicated-key group are flagged (not only the second occurrence).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.silver_common import (
    DUPLICATE_CUSTOMER_ID,
    DUPLICATE_ORDER_ID,
    DUPLICATE_PRODUCT_ID,
    append_validation_reasons,
)


def _flag_duplicate_primary_key(df: DataFrame, pk_column: str, reason_code: str) -> DataFrame:
    """Flag every row whose non-null primary-key value appears more than once."""
    pk_count = F.count(F.col(pk_column)).over(Window.partitionBy(pk_column))
    duplicate_condition = (pk_count > 1) & F.col(pk_column).isNotNull()
    return append_validation_reasons(df, [(duplicate_condition, reason_code)])


def validate_customers_uniqueness(df: DataFrame) -> DataFrame:
    return _flag_duplicate_primary_key(df, "customer_id", DUPLICATE_CUSTOMER_ID)


def validate_products_uniqueness(df: DataFrame) -> DataFrame:
    return _flag_duplicate_primary_key(df, "product_id", DUPLICATE_PRODUCT_ID)


def validate_orders_uniqueness(df: DataFrame) -> DataFrame:
    return _flag_duplicate_primary_key(df, "order_id", DUPLICATE_ORDER_ID)


def apply_uniqueness_validation(entity: str, df: DataFrame) -> DataFrame:
    validators = {
        "customers": validate_customers_uniqueness,
        "products": validate_products_uniqueness,
        "orders": validate_orders_uniqueness,
    }
    if entity not in validators:
        raise ValueError(f"Unknown entity for uniqueness validation: {entity}")
    return validators[entity](df)
