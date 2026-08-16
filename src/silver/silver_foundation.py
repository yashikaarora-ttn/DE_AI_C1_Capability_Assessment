"""
Orchestrate Silver validation: completeness → type → uniqueness → referential integrity.
"""

from __future__ import annotations

import importlib

from pyspark.sql import DataFrame

from silver.silver_common import add_silver_processed_timestamp, finalize_dq_columns

_completeness = importlib.import_module("silver.01_quality_completeness")
_type_validation = importlib.import_module("silver.03_quality_type_validation")
_uniqueness = importlib.import_module("silver.02_quality_uniqueness")
_referential_integrity = importlib.import_module("silver.04_quality_referential_integrity")


def apply_silver_validation(
    entity: str,
    bronze_df: DataFrame,
    customers_silver: DataFrame | None = None,
    products_silver: DataFrame | None = None,
) -> DataFrame:
    """
    Apply full Silver validation pipeline for one entity.

    For orders, pass customers_silver and products_silver (validated through uniqueness)
    so referential integrity can validate FK references.
    """
    df = _completeness.apply_completeness_validation(entity, bronze_df)
    df = _type_validation.apply_type_validation(entity, df)
    if entity == "orders":
        df = _type_validation.drop_order_fk_raw_columns(df)
    df = _uniqueness.apply_uniqueness_validation(entity, df)
    df = _referential_integrity.apply_referential_integrity_validation(
        entity, df, customers_silver, products_silver
    )
    df = finalize_dq_columns(df)
    df = add_silver_processed_timestamp(df)
    return df


def apply_silver_foundation(entity: str, bronze_df: DataFrame) -> DataFrame:
    """
    Apply completeness and type/business validation only (no uniqueness or RI).

    Kept for backward compatibility with foundation-only tests.
    """
    df = _completeness.apply_completeness_validation(entity, bronze_df)
    df = _type_validation.apply_type_validation(entity, df)
    if entity == "orders":
        df = _type_validation.drop_order_fk_raw_columns(df)
    df = finalize_dq_columns(df)
    df = add_silver_processed_timestamp(df)
    return df


def apply_silver_pipeline(
    bronze_customers: DataFrame,
    bronze_products: DataFrame,
    bronze_orders: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Run full Silver validation for all entities in dependency order."""
    customers = apply_silver_validation("customers", bronze_customers)
    products = apply_silver_validation("products", bronze_products)
    orders = apply_silver_validation(
        "orders",
        bronze_orders,
        customers_silver=customers,
        products_silver=products,
    )
    return customers, products, orders


def apply_silver_all(bronze_dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Run full Silver pipeline from a dict of Bronze DataFrames."""
    customers, products, orders = apply_silver_pipeline(
        bronze_dfs["customers"],
        bronze_dfs["products"],
        bronze_dfs["orders"],
    )
    return {"customers": customers, "products": products, "orders": orders}
