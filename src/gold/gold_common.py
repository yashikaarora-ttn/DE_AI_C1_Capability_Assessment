"""
Gold aggregation utilities — trusted Silver filtering and business metrics.

Trusted-data policy
---------------------
- Entity rows: `dq_status = 'PASS'` only.
- Realized revenue metrics: trusted orders with `order_status = 'Completed'`.
- Trusted business orders: PASS Completed orders that inner-join PASS customer and PASS product.
  Gold realized metrics use this set so product- and customer-level revenue reconcile.

Join behavior: inner joins for sales-by-product exclude completed PASS orders whose
customer or product failed Silver (or vice versa). Revenue-by-customer includes all
PASS customers with zero metrics when no business orders exist.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.silver_common import DQ_STATUS_PASS

COMPLETED_ORDER_STATUS = "Completed"

SEGMENT_INACTIVE = "Inactive"
SEGMENT_ONE_TIME = "One-Time"
SEGMENT_HIGH_VALUE = "High-Value"
SEGMENT_REPEAT = "Repeat"

PERIOD_DAILY = "DAILY"
PERIOD_WEEKLY = "WEEKLY"


def trusted_silver(df: DataFrame) -> DataFrame:
    """Rows that passed all implemented Silver validation rules."""
    return df.filter(F.col("dq_status") == DQ_STATUS_PASS)


def trusted_completed_orders(orders_df: DataFrame) -> DataFrame:
    """PASS Silver orders with Completed status — basis for realized revenue."""
    return trusted_silver(orders_df).filter(
        F.col("order_status") == COMPLETED_ORDER_STATUS
    )


def trusted_business_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """
    Completed PASS orders that join to PASS customer and PASS product.

    Gold realized metrics use this set so product- and customer-level revenue reconcile.
    A PASS order is excluded when its customer or product failed Silver (untrusted join).
    """
    completed = trusted_completed_orders(orders_df)
    trusted_customers = trusted_silver(customers_df).select("customer_id")
    trusted_products = trusted_silver(products_df).select("product_id")
    return (
        completed.join(trusted_customers, on="customer_id", how="inner")
        .join(trusted_products, on="product_id", how="inner")
    )


def _customer_order_stats(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Per-customer aggregates from trusted business orders."""
    return trusted_business_orders(orders_df, customers_df, products_df).groupBy(
        "customer_id"
    ).agg(
        F.count(F.lit(1)).alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
    )


def build_sales_by_product(
    orders_df: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    """
    Sales metrics by product from trusted business orders and trusted products.

    Inner joins: only products with at least one qualifying order appear.
    """
    business = trusted_business_orders(orders_df, customers_df, products_df)
    products = trusted_silver(products_df).select(
        "product_id", "product_name", "category"
    )
    joined = business.join(products, on="product_id", how="inner")
    return joined.groupBy("product_id", "product_name", "category").agg(
        F.count(F.lit(1)).alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
    )


def build_revenue_by_customer(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """
    Revenue metrics per trusted customer.

    Includes all PASS customers; those without trusted business orders have zero metrics.
    lifetime_value_actual equals sum of trusted business order amounts (observed).
    """
    customers = trusted_silver(customers_df).select(
        "customer_id", "customer_name", "customer_segment"
    )
    stats = _customer_order_stats(orders_df, customers_df, products_df)
    joined = customers.join(stats, on="customer_id", how="left")
    joined = joined.fillna(0, subset=["total_orders", "total_revenue"])
    joined = joined.withColumn(
        "avg_order_value",
        F.when(F.col("total_orders") > 0, F.col("avg_order_value")).otherwise(F.lit(0.0)),
    )
    return joined.withColumn("lifetime_value_actual", F.col("total_revenue"))


def build_customer_segmentation(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    """
    Mutually exclusive segments over trusted customers.

    Rules (evaluated on trusted business-order counts/revenue):
    - Inactive: zero business orders
    - One-Time: exactly one business order
    - High-Value: more than one AND total_revenue >= threshold
    - Repeat: more than one AND total_revenue < threshold
    """
    customer_metrics = build_revenue_by_customer(orders_df, customers_df, products_df)
    segmented = customer_metrics.withColumn(
        "segment_type",
        F.when(F.col("total_orders") == 0, SEGMENT_INACTIVE)
        .when(F.col("total_orders") == 1, SEGMENT_ONE_TIME)
        .when(
            (F.col("total_orders") > 1) & (F.col("total_revenue") >= high_value_threshold),
            SEGMENT_HIGH_VALUE,
        )
        .when(F.col("total_orders") > 1, SEGMENT_REPEAT)
        .otherwise(SEGMENT_INACTIVE),
    )
    return segmented.groupBy("segment_type").agg(
        F.count("*").alias("customer_count"),
        F.avg("total_revenue").alias("avg_revenue"),
        F.sum("total_revenue").alias("total_revenue"),
    )


def build_daily_weekly_trends(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Daily and weekly order/revenue trends from trusted business orders."""
    business = trusted_business_orders(orders_df, customers_df, products_df)
    daily = business.groupBy(F.col("order_date").alias("period_start")).agg(
        F.count(F.lit(1)).alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
    ).withColumn("period_type", F.lit(PERIOD_DAILY))

    weekly = business.withColumn(
        "period_start",
        F.to_date(F.date_trunc("week", F.col("order_date"))),
    ).groupBy("period_start").agg(
        F.count(F.lit(1)).alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
    ).withColumn("period_type", F.lit(PERIOD_WEEKLY))

    return daily.unionByName(weekly)


def total_trusted_business_revenue(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> float:
    """Sum of total_amount for trusted business orders (reconciliation helper)."""
    row = trusted_business_orders(orders_df, customers_df, products_df).agg(
        F.sum("total_amount").alias("revenue")
    ).collect()[0]
    return float(row["revenue"] or 0.0)
