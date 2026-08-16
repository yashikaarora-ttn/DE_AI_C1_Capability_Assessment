"""
Business-rule validation for Silver (assignment module alignment).

Business rules are implemented in `03_quality_type_validation.py` alongside type
normalization to avoid duplicate rule application and regression risk. This module
re-exports those validators for a clear assignment file mapping.
"""

from __future__ import annotations

import importlib

from pyspark.sql import DataFrame

_type_validation = importlib.import_module("silver.03_quality_type_validation")

validate_customers_business_rules = _type_validation.validate_customers_type_rules
validate_products_business_rules = _type_validation.validate_products_type_rules
validate_orders_business_rules = _type_validation.validate_orders_type_rules


def apply_business_logic_validation(entity: str, df: DataFrame) -> DataFrame:
    """Apply business rules for the entity (delegates to type validation module)."""
    return _type_validation.apply_type_validation(entity, df)
