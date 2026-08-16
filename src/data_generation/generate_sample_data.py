"""
Generate deterministic sample CSV datasets for the e-commerce Medallion pipeline.

Duplicate row definition (used by generator and tests)
------------------------------------------------------
For a primary-key column, the *duplicate row count* is the number of rows whose
key value appears more than once in the dataset. Every row belonging to a
duplicated key is counted, not just the "extra" copies.

Examples:
  customer_id values [1, 1, 2, 3]  -> duplicate row count = 2 (both rows with id 1)
  order_id values [10, 10, 10, 11]   -> duplicate row count = 3 (all rows with id 10)

Assessment targets:
  - 10 duplicate customer_id rows  -> 5 customer_ids each appearing exactly twice
  - 20 duplicate order_id rows     -> 10 order_ids each appearing exactly twice

Intentional DQ issue categories (orders) are DISJOINT row sets:
  - NULL customer_id, NULL product_id, invalid customer_id, invalid product_id,
    and duplicate order_id rows do not overlap.
"""

from __future__ import annotations

import argparse
import random
import re
import sys
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

NUM_CUSTOMERS = 10_000
NUM_PRODUCTS = 500
NUM_ORDERS = 100_000

NULL_EMAIL_COUNT = 50
DUPLICATE_CUSTOMER_ID_ROWS = 10
DUPLICATE_CUSTOMER_ID_PAIRS = DUPLICATE_CUSTOMER_ID_ROWS // 2  # 5 ids × 2 rows

NULL_ORDER_CUSTOMER_ID_COUNT = 100
NULL_ORDER_PRODUCT_ID_COUNT = 200
INVALID_ORDER_CUSTOMER_ID_COUNT = 50
INVALID_ORDER_PRODUCT_ID_COUNT = 30
DUPLICATE_ORDER_ID_ROWS = 20
DUPLICATE_ORDER_ID_PAIRS = DUPLICATE_ORDER_ID_ROWS // 2  # 10 ids × 2 rows

INVALID_CUSTOMER_ID_START = 90_001
INVALID_PRODUCT_ID_START = 9_001

CLEAN_ORDER_COUNT = (
    NUM_ORDERS
    - DUPLICATE_ORDER_ID_ROWS
    - NULL_ORDER_CUSTOMER_ID_COUNT
    - NULL_ORDER_PRODUCT_ID_COUNT
    - INVALID_ORDER_CUSTOMER_ID_COUNT
    - INVALID_ORDER_PRODUCT_ID_COUNT
)

CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
ORDER_STATUSES = ("Pending", "Completed", "Cancelled")
PRODUCT_CATEGORIES = (
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Beauty",
    "Toys",
    "Food",
)

DEFAULT_SEED = 42
DEFAULT_OUTPUT_DIR = "data"

CUSTOMERS_FILENAME = "customers.csv"
PRODUCTS_FILENAME = "products.csv"
ORDERS_FILENAME = "orders.csv"

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def find_repo_root(start: Optional[Path] = None) -> Path:
    """Resolve repository root by walking up from this file."""
    current = (start or Path(__file__).resolve()).parent
    for candidate in [current, *current.parents]:
        if (candidate / "README.md").exists() and (candidate / "src").is_dir():
            return candidate
    raise FileNotFoundError(
        "Could not locate repository root (expected README.md and src/ directory)."
    )


def resolve_output_dir(output_dir: Optional[str] = None) -> Path:
    """Return absolute output directory path relative to repo root."""
    root = find_repo_root()
    target = Path(output_dir or DEFAULT_OUTPUT_DIR)
    if target.is_absolute():
        return target
    return root / target


# ---------------------------------------------------------------------------
# DQ counting helpers (shared with tests)
# ---------------------------------------------------------------------------


def count_duplicate_key_rows(df: pd.DataFrame, key_column: str) -> int:
    """
    Count rows whose key value appears more than once in the dataframe.
    See module docstring for the formal duplicate row definition.
    """
    if key_column not in df.columns:
        raise ValueError(f"Column '{key_column}' not found in dataframe.")
    counts = df[key_column].value_counts(dropna=False)
    duplicated_keys = counts[counts > 1].index
    return int(df[key_column].isin(duplicated_keys).sum())


def count_null_emails(df: pd.DataFrame) -> int:
    """Count customer rows with NULL/empty email."""
    if "email" not in df.columns:
        raise ValueError("Column 'email' not found in dataframe.")
    series = df["email"]
    return int(series.isna().sum() + (series == "").sum())


def count_null_column(df: pd.DataFrame, column: str) -> int:
    """Count NULL values in a column."""
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in dataframe.")
    return int(df[column].isna().sum())


def count_invalid_foreign_keys(
    child_df: pd.DataFrame,
    child_column: str,
    parent_df: pd.DataFrame,
    parent_column: str,
) -> int:
    """
    Count non-null child FK values that do not exist in the parent key set.
    """
    if child_column not in child_df.columns:
        raise ValueError(f"Column '{child_column}' not found in child dataframe.")
    if parent_column not in parent_df.columns:
        raise ValueError(f"Column '{parent_column}' not found in parent dataframe.")
    child_values = child_df[child_column]
    non_null = child_values.dropna()
    parent_keys: Set[Any] = set(parent_df[parent_column].dropna().unique())
    is_valid = non_null.isin(parent_keys)
    return int((~is_valid).sum())


def decimal_equal(a: float, b: float, tolerance: float = 0.01) -> bool:
    """Compare decimal values within tolerance."""
    return abs(float(a) - float(b)) <= tolerance


def money(value: float) -> Decimal:
    """Round monetary values to two decimal places."""
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Generation helpers
# ---------------------------------------------------------------------------


def _random_past_date(rng: random.Random, start_year: int = 2018) -> date:
    today = date.today()
    start = date(start_year, 1, 1)
    days_between = (today - start).days
    if days_between <= 0:
        return today
    return start + timedelta(days=rng.randint(0, days_between))


def _payment_date_for_status(
    rng: random.Random, order_date: date, status: str
) -> Optional[date]:
    today = date.today()
    if status == "Completed":
        offset = rng.randint(0, 5)
        payment = order_date + timedelta(days=offset)
        return min(payment, today)
    if status == "Cancelled":
        if rng.random() < 0.7:
            return None
        return min(order_date, today)
    return None


def generate_customers(rng: random.Random, faker: Faker) -> pd.DataFrame:
    """Generate 10,000 customer rows with intentional NULL emails and duplicate IDs."""
    rows: List[Dict[str, Any]] = []

    # 9,995 unique customer_ids (1..9995)
    for customer_id in range(1, NUM_CUSTOMERS - DUPLICATE_CUSTOMER_ID_PAIRS + 1):
        segment = rng.choice(CUSTOMER_SEGMENTS)
        lifetime = money(rng.uniform(100, 50_000) * {"Premium": 2.5, "Standard": 1.5, "Basic": 1.0}[segment])
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": faker.name(),
                "email": faker.email(),
                "country": faker.country(),
                "signup_date": _random_past_date(rng).isoformat(),
                "customer_segment": segment,
                "lifetime_value": float(lifetime),
            }
        )

    # 5 duplicate customer_id rows (ids 1..5 appear twice -> 10 duplicate rows total)
    for customer_id in range(1, DUPLICATE_CUSTOMER_ID_PAIRS + 1):
        segment = rng.choice(CUSTOMER_SEGMENTS)
        lifetime = money(rng.uniform(100, 50_000))
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": faker.name(),
                "email": faker.email(),
                "country": faker.country(),
                "signup_date": _random_past_date(rng).isoformat(),
                "customer_segment": segment,
                "lifetime_value": float(lifetime),
            }
        )

    df = pd.DataFrame(rows)

    # NULL emails on 50 rows — avoid duplicate-id rows (ids 1..5) to keep categories disjoint
    null_email_candidates = df.index[
        ~df["customer_id"].isin(range(1, DUPLICATE_CUSTOMER_ID_PAIRS + 1))
    ].tolist()
    rng.shuffle(null_email_candidates)
    if len(null_email_candidates) < NULL_EMAIL_COUNT:
        raise ValueError(
            f"Not enough candidates for NULL emails: need {NULL_EMAIL_COUNT}, "
            f"have {len(null_email_candidates)}."
        )
    null_email_indices = null_email_candidates[:NULL_EMAIL_COUNT]
    df.loc[null_email_indices, "email"] = None

    _validate_customer_frame(df)
    return df


def generate_products(rng: random.Random, faker: Faker) -> pd.DataFrame:
    """Generate 500 product rows."""
    rows: List[Dict[str, Any]] = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        price = money(rng.uniform(5, 500))
        cost = money(float(price) * rng.uniform(0.3, 0.85))
        rows.append(
            {
                "product_id": product_id,
                "product_name": faker.catch_phrase(),
                "category": rng.choice(PRODUCT_CATEGORIES),
                "price": float(price),
                "cost": float(cost),
                "stock_quantity": rng.randint(0, 2_000),
                "reorder_level": rng.randint(10, 200),
            }
        )
    df = pd.DataFrame(rows)
    _validate_product_frame(df)
    return df


def generate_orders(
    rng: random.Random,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate 100,000 order rows with disjoint intentional DQ issue categories."""
    valid_customer_ids = customers_df["customer_id"].unique().tolist()
    valid_product_ids = products_df["product_id"].unique().tolist()
    product_price_map = products_df.set_index("product_id")["price"].to_dict()

    rows: List[Dict[str, Any]] = []
    order_id = 1

    def append_order(
        customer_id: Optional[int],
        product_id: Optional[int],
        oid: int,
    ) -> None:
        product_id_for_price = (
            product_id if product_id in product_price_map else rng.choice(valid_product_ids)
        )
        unit_price = money(product_price_map[product_id_for_price])
        quantity = rng.randint(1, 10)
        total_amount = money(float(unit_price) * quantity)
        order_date = _random_past_date(rng, start_year=2019)
        status = rng.choice(ORDER_STATUSES)
        payment_date = _payment_date_for_status(rng, order_date, status)
        rows.append(
            {
                "order_id": oid,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": float(unit_price),
                "total_amount": float(total_amount),
                "order_status": status,
                "payment_date": payment_date.isoformat() if payment_date else None,
            }
        )

    # Clean orders: unique order_ids, valid FKs
    for _ in range(CLEAN_ORDER_COUNT):
        append_order(
            rng.choice(valid_customer_ids),
            rng.choice(valid_product_ids),
            order_id,
        )
        order_id += 1

    # Duplicate order_id rows: 10 ids each appearing twice (20 duplicate rows)
    duplicate_base_id = order_id
    for dup_id in range(duplicate_base_id, duplicate_base_id + DUPLICATE_ORDER_ID_PAIRS):
        for _ in range(2):
            append_order(
                rng.choice(valid_customer_ids),
                rng.choice(valid_product_ids),
                dup_id,
            )
    order_id = duplicate_base_id + DUPLICATE_ORDER_ID_PAIRS

    # NULL customer_id (100 rows) — disjoint from duplicates above
    for _ in range(NULL_ORDER_CUSTOMER_ID_COUNT):
        append_order(None, rng.choice(valid_product_ids), order_id)
        order_id += 1

    # NULL product_id (200 rows)
    for _ in range(NULL_ORDER_PRODUCT_ID_COUNT):
        append_order(rng.choice(valid_customer_ids), None, order_id)
        order_id += 1

    # Invalid customer_id (50 rows, non-null)
    for offset in range(INVALID_ORDER_CUSTOMER_ID_COUNT):
        append_order(
            INVALID_CUSTOMER_ID_START + offset,
            rng.choice(valid_product_ids),
            order_id,
        )
        order_id += 1

    # Invalid product_id (30 rows, non-null)
    for offset in range(INVALID_ORDER_PRODUCT_ID_COUNT):
        append_order(
            rng.choice(valid_customer_ids),
            INVALID_PRODUCT_ID_START + offset,
            order_id,
        )
        order_id += 1

    df = pd.DataFrame(rows)
    _validate_order_frame(df, customers_df, products_df)
    return df


def _validate_customer_frame(df: pd.DataFrame) -> None:
    if len(df) != NUM_CUSTOMERS:
        raise ValueError(f"Expected {NUM_CUSTOMERS} customers, got {len(df)}.")
    dup_rows = count_duplicate_key_rows(df, "customer_id")
    if dup_rows != DUPLICATE_CUSTOMER_ID_ROWS:
        raise ValueError(
            f"Expected {DUPLICATE_CUSTOMER_ID_ROWS} duplicate customer_id rows, got {dup_rows}."
        )
    null_emails = count_null_emails(df)
    if null_emails != NULL_EMAIL_COUNT:
        raise ValueError(f"Expected {NULL_EMAIL_COUNT} NULL emails, got {null_emails}.")
    non_null_emails = df["email"].dropna()
    non_null_emails = non_null_emails[non_null_emails != ""]
    if not non_null_emails.map(lambda e: bool(EMAIL_PATTERN.match(str(e)))).all():
        raise ValueError("Non-null customer emails must match a basic email format.")
    today_str = date.today().isoformat()
    if df["signup_date"].max() > today_str:
        raise ValueError("signup_date must not be in the future.")


def _validate_product_frame(df: pd.DataFrame) -> None:
    if len(df) != NUM_PRODUCTS:
        raise ValueError(f"Expected {NUM_PRODUCTS} products, got {len(df)}.")
    if df["product_id"].duplicated().any():
        raise ValueError("product_id must be unique in products dataset.")


def _validate_order_frame(
    df: pd.DataFrame,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> None:
    if len(df) != NUM_ORDERS:
        raise ValueError(f"Expected {NUM_ORDERS} orders, got {len(df)}.")

    if count_duplicate_key_rows(df, "order_id") != DUPLICATE_ORDER_ID_ROWS:
        raise ValueError("Duplicate order_id row count does not match target.")
    if count_null_column(df, "customer_id") != NULL_ORDER_CUSTOMER_ID_COUNT:
        raise ValueError("NULL customer_id count does not match target.")
    if count_null_column(df, "product_id") != NULL_ORDER_PRODUCT_ID_COUNT:
        raise ValueError("NULL product_id count does not match target.")
    if count_invalid_foreign_keys(df, "customer_id", customers_df, "customer_id") != INVALID_ORDER_CUSTOMER_ID_COUNT:
        raise ValueError("Invalid customer_id count does not match target.")
    if count_invalid_foreign_keys(df, "product_id", products_df, "product_id") != INVALID_ORDER_PRODUCT_ID_COUNT:
        raise ValueError("Invalid product_id count does not match target.")

    today_str = date.today().isoformat()
    if df["order_date"].max() > today_str:
        raise ValueError("order_date must not be in the future.")
    payment_dates = df["payment_date"].dropna()
    if len(payment_dates) > 0 and payment_dates.max() > today_str:
        raise ValueError("payment_date must not be in the future.")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write dataframe to CSV with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, na_rep="")


def generate_all(
    seed: int = DEFAULT_SEED,
    output_dir: Optional[str] = None,
) -> Tuple[Path, Path, Path]:
    """
    Generate all sample datasets and write CSV files.

    Returns paths to customers, products, and orders CSV files.
    """
    if seed < 0:
        raise ValueError(f"Seed must be non-negative, got {seed}.")

    rng = random.Random(seed)
    faker = Faker()
    faker.seed_instance(seed)

    out_dir = resolve_output_dir(output_dir)

    customers_df = generate_customers(rng, faker)
    products_df = generate_products(rng, faker)
    orders_df = generate_orders(rng, customers_df, products_df)

    customers_path = out_dir / CUSTOMERS_FILENAME
    products_path = out_dir / PRODUCTS_FILENAME
    orders_path = out_dir / ORDERS_FILENAME

    write_csv(customers_df, customers_path)
    write_csv(products_df, products_path)
    write_csv(orders_df, orders_path)

    return customers_path, products_path, orders_path


def build_summary(
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    orders_df: pd.DataFrame,
) -> Dict[str, int]:
    """Build a summary dict of row counts and DQ issue counts."""
    return {
        "customers_rows": len(customers_df),
        "products_rows": len(products_df),
        "orders_rows": len(orders_df),
        "null_emails": count_null_emails(customers_df),
        "duplicate_customer_id_rows": count_duplicate_key_rows(customers_df, "customer_id"),
        "null_order_customer_id": count_null_column(orders_df, "customer_id"),
        "null_order_product_id": count_null_column(orders_df, "product_id"),
        "invalid_order_customer_id": count_invalid_foreign_keys(
            orders_df, "customer_id", customers_df, "customer_id"
        ),
        "invalid_order_product_id": count_invalid_foreign_keys(
            orders_df, "product_id", products_df, "product_id"
        ),
        "duplicate_order_id_rows": count_duplicate_key_rows(orders_df, "order_id"),
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate sample e-commerce CSV datasets for the Medallion pipeline."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducible output (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory relative to repo root (default: {DEFAULT_OUTPUT_DIR}).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        customers_path, products_path, orders_path = generate_all(
            seed=args.seed,
            output_dir=args.output_dir,
        )
    except (ValueError, FileNotFoundError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    customers_df = pd.read_csv(customers_path)
    products_df = pd.read_csv(products_path)
    orders_df = pd.read_csv(orders_path)
    summary = build_summary(customers_df, products_df, orders_df)

    print("Sample data generated successfully.")
    print(f"  customers: {customers_path}")
    print(f"  products:  {products_path}")
    print(f"  orders:    {orders_path}")
    print("Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
