"""
Silver referential integrity — order FKs must reference existing customer and product keys.

NULL FKs are completeness failures only. Malformed FK strings flagged with type codes
are excluded from RI checks.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.silver_common import (
    INVALID_CUSTOMER_ID,
    INVALID_CUSTOMER_ID_TYPE,
    INVALID_PRODUCT_ID,
    INVALID_PRODUCT_ID_TYPE,
    append_validation_reasons,
)


def _customer_ri_condition(valid_customer_col: str) -> F.Column:
    """Non-null normalized customer_id not present in parent customers."""
    return (
        F.col("customer_id").isNotNull()
        & ~F.array_contains(F.col("dq_failure_reasons"), INVALID_CUSTOMER_ID_TYPE)
        & F.col(valid_customer_col).isNull()
    )


def _product_ri_condition(valid_product_col: str) -> F.Column:
    """Non-null normalized product_id not present in parent products."""
    return (
        F.col("product_id").isNotNull()
        & ~F.array_contains(F.col("dq_failure_reasons"), INVALID_PRODUCT_ID_TYPE)
        & F.col(valid_product_col).isNull()
    )


def validate_orders_referential_integrity(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """
    Validate order FKs against Silver customer and product key sets.

    Uses broadcast left joins on normalized INTEGER FK columns.
    """
    valid_customers = customers_df.select(
        F.col("customer_id").alias("_valid_customer_id")
    ).distinct()
    valid_products = products_df.select(
        F.col("product_id").alias("_valid_product_id")
    ).distinct()

    df = orders_df.join(
        F.broadcast(valid_customers),
        orders_df["customer_id"] == valid_customers["_valid_customer_id"],
        how="left",
    )
    df = df.join(
        F.broadcast(valid_products),
        df["product_id"] == valid_products["_valid_product_id"],
        how="left",
    )

    df = append_validation_reasons(
        df,
        [
            (_customer_ri_condition("_valid_customer_id"), INVALID_CUSTOMER_ID),
            (_product_ri_condition("_valid_product_id"), INVALID_PRODUCT_ID),
        ],
    )
    return df.drop("_valid_customer_id", "_valid_product_id")


def apply_referential_integrity_validation(
    entity: str,
    df: DataFrame,
    customers_df: DataFrame | None = None,
    products_df: DataFrame | None = None,
) -> DataFrame:
    if entity != "orders":
        return df
    if customers_df is None or products_df is None:
        return df
    return validate_orders_referential_integrity(df, customers_df, products_df)
