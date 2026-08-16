"""Shared Spark schemas for Silver unit tests."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from bronze.bronze_common import CUSTOMERS_CSV_SCHEMA, ORDERS_CSV_SCHEMA

SAMPLE_CUSTOMER_ROW = (
    1,
    "Alice",
    "alice@example.com",
    "US",
    date(2020, 1, 1),
    "Basic",
    Decimal("100.00"),
)

SAMPLE_ORDER_ROW = (
    1,
    "100",
    date(2020, 1, 1),
    "10",
    2,
    Decimal("10.00"),
    Decimal("20.00"),
    "Completed",
    date(2020, 1, 2),
)
