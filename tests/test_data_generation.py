"""Tests for sample data generation."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pandas as pd
import pytest
from faker import Faker

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from data_generation.generate_sample_data import (  # noqa: E402
    CLEAN_ORDER_COUNT,
    CUSTOMER_SEGMENTS,
    DUPLICATE_CUSTOMER_ID_ROWS,
    DUPLICATE_ORDER_ID_ROWS,
    DEFAULT_SEED,
    EMAIL_PATTERN,
    INVALID_ORDER_CUSTOMER_ID_COUNT,
    INVALID_ORDER_PRODUCT_ID_COUNT,
    NULL_EMAIL_COUNT,
    NULL_ORDER_CUSTOMER_ID_COUNT,
    NULL_ORDER_PRODUCT_ID_COUNT,
    NUM_CUSTOMERS,
    NUM_ORDERS,
    NUM_PRODUCTS,
    ORDER_STATUSES,
    count_duplicate_key_rows,
    count_invalid_foreign_keys,
    count_null_column,
    count_null_emails,
    decimal_equal,
    generate_all,
    generate_customers,
    generate_orders,
    generate_products,
)


@pytest.fixture
def seeded_rng() -> random.Random:
    return random.Random(DEFAULT_SEED)


@pytest.fixture
def seeded_faker() -> Faker:
    faker = Faker()
    faker.seed_instance(DEFAULT_SEED)
    return faker


@pytest.fixture
def generated_data(seeded_rng: random.Random, seeded_faker: Faker):
    customers = generate_customers(seeded_rng, seeded_faker)
    products = generate_products(seeded_rng, seeded_faker)
    orders = generate_orders(seeded_rng, customers, products)
    return customers, products, orders


def test_exact_row_counts(generated_data):
    customers, products, orders = generated_data
    assert len(customers) == NUM_CUSTOMERS
    assert len(products) == NUM_PRODUCTS
    assert len(orders) == NUM_ORDERS


def test_exactly_50_null_customer_emails(generated_data):
    customers, _, _ = generated_data
    assert count_null_emails(customers) == NULL_EMAIL_COUNT


def test_duplicate_customer_id_row_definition_and_count(generated_data):
    """
    Duplicate row definition: every row whose customer_id appears more than once
    is counted. Target: 5 ids each appearing twice -> 10 duplicate rows.
    """
    customers, _, _ = generated_data
    assert count_duplicate_key_rows(customers, "customer_id") == DUPLICATE_CUSTOMER_ID_ROWS
    counts = customers["customer_id"].value_counts()
    duplicated = counts[counts > 1]
    assert duplicated.sum() == DUPLICATE_CUSTOMER_ID_ROWS
    assert len(duplicated) == DUPLICATE_CUSTOMER_ID_ROWS // 2


def test_exactly_100_null_order_customer_id(generated_data):
    _, _, orders = generated_data
    assert count_null_column(orders, "customer_id") == NULL_ORDER_CUSTOMER_ID_COUNT


def test_exactly_200_null_order_product_id(generated_data):
    _, _, orders = generated_data
    assert count_null_column(orders, "product_id") == NULL_ORDER_PRODUCT_ID_COUNT


def test_exactly_50_invalid_customer_references(generated_data):
    customers, _, orders = generated_data
    assert (
        count_invalid_foreign_keys(orders, "customer_id", customers, "customer_id")
        == INVALID_ORDER_CUSTOMER_ID_COUNT
    )


def test_exactly_30_invalid_product_references(generated_data):
    _, products, orders = generated_data
    assert (
        count_invalid_foreign_keys(orders, "product_id", products, "product_id")
        == INVALID_ORDER_PRODUCT_ID_COUNT
    )


def test_duplicate_order_id_row_definition_and_count(generated_data):
    """
    Duplicate row definition: every row whose order_id appears more than once
    is counted. Target: 10 ids each appearing twice -> 20 duplicate rows.
    """
    _, _, orders = generated_data
    assert count_duplicate_key_rows(orders, "order_id") == DUPLICATE_ORDER_ID_ROWS
    counts = orders["order_id"].value_counts()
    duplicated = counts[counts > 1]
    assert duplicated.sum() == DUPLICATE_ORDER_ID_ROWS
    assert len(duplicated) == DUPLICATE_ORDER_ID_ROWS // 2


def test_valid_foreign_keys_after_excluding_intentional_issues(generated_data):
    customers, products, orders = generated_data
    # Exclude NULL FKs, invalid FKs, and duplicate order_id rows
    valid_orders = orders[
        orders["customer_id"].notna()
        & orders["product_id"].notna()
        & orders["customer_id"].isin(customers["customer_id"])
        & orders["product_id"].isin(products["product_id"])
        & ~orders["order_id"].duplicated(keep=False)
    ]
    assert len(valid_orders) == CLEAN_ORDER_COUNT
    assert count_invalid_foreign_keys(
        valid_orders, "customer_id", customers, "customer_id"
    ) == 0
    assert count_invalid_foreign_keys(
        valid_orders, "product_id", products, "product_id"
    ) == 0


def test_total_amount_equals_quantity_times_unit_price(generated_data):
    _, _, orders = generated_data
    for _, row in orders.iterrows():
        expected = float(row["quantity"]) * float(row["unit_price"])
        assert decimal_equal(row["total_amount"], expected)


def test_allowed_customer_segments_and_order_statuses(generated_data):
    customers, _, orders = generated_data
    assert set(customers["customer_segment"].unique()).issubset(set(CUSTOMER_SEGMENTS))
    assert set(orders["order_status"].unique()).issubset(set(ORDER_STATUSES))


def test_same_seed_produces_reproducible_output(tmp_path):
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    generate_all(seed=DEFAULT_SEED, output_dir=str(out1))
    generate_all(seed=DEFAULT_SEED, output_dir=str(out2))

    for name in ("customers.csv", "products.csv", "orders.csv"):
        df1 = pd.read_csv(out1 / name)
        df2 = pd.read_csv(out2 / name)
        pd.testing.assert_frame_equal(df1, df2)


def test_clean_order_count_constant():
    assert CLEAN_ORDER_COUNT == 99_600


def test_order_dq_issue_categories_are_disjoint(generated_data):
    customers, products, orders = generated_data
    valid_customer_ids = set(customers["customer_id"])
    valid_product_ids = set(products["product_id"])

    null_customer = orders["customer_id"].isna()
    null_product = orders["product_id"].isna()
    invalid_customer = orders["customer_id"].notna() & ~orders["customer_id"].isin(valid_customer_ids)
    invalid_product = orders["product_id"].notna() & ~orders["product_id"].isin(valid_product_ids)
    duplicate_order = orders.duplicated(subset=["order_id"], keep=False)

    categories = [
        ("null_customer_id", null_customer),
        ("null_product_id", null_product),
        ("invalid_customer_id", invalid_customer),
        ("invalid_product_id", invalid_product),
        ("duplicate_order_id", duplicate_order),
    ]

    for i in range(len(categories)):
        for j in range(i + 1, len(categories)):
            name_i, mask_i = categories[i]
            name_j, mask_j = categories[j]
            overlap = int((mask_i & mask_j).sum())
            assert overlap == 0, f"{name_i} overlaps {name_j} by {overlap} rows"

    assert int(null_customer.sum()) == NULL_ORDER_CUSTOMER_ID_COUNT
    assert int(null_product.sum()) == NULL_ORDER_PRODUCT_ID_COUNT
    assert int(invalid_customer.sum()) == INVALID_ORDER_CUSTOMER_ID_COUNT
    assert int(invalid_product.sum()) == INVALID_ORDER_PRODUCT_ID_COUNT
    assert int(duplicate_order.sum()) == DUPLICATE_ORDER_ID_ROWS


def test_customer_null_email_disjoint_from_duplicate_customer_ids(generated_data):
    customers, _, _ = generated_data
    duplicate_customer = customers.duplicated(subset=["customer_id"], keep=False)
    null_email = customers["email"].isna() | (customers["email"] == "")
    assert int((duplicate_customer & null_email).sum()) == 0


def test_non_null_emails_have_basic_format(generated_data):
    customers, _, _ = generated_data
    emails = customers["email"].dropna()
    emails = emails[emails != ""]
    assert all(EMAIL_PATTERN.match(str(email)) for email in emails)


def test_dates_are_not_in_the_future(generated_data):
    from datetime import date

    customers, _, orders = generated_data
    today = date.today().isoformat()

    assert customers["signup_date"].max() <= today
    assert orders["order_date"].max() <= today
    payment_dates = orders["payment_date"].dropna()
    if len(payment_dates) > 0:
        assert payment_dates.max() <= today
