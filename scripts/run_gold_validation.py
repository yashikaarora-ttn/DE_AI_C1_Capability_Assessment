"""Run Gold validation on generated data and print metrics for Phase 4 report."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from bronze.bronze_common import BronzeConfig, prepare_bronze_dataframe  # noqa: E402
from data_generation.generate_sample_data import generate_all  # noqa: E402
from gold.create_gold_tables import build_all_gold_dfs  # noqa: E402
from gold.gold_common import (  # noqa: E402
    PERIOD_DAILY,
    PERIOD_WEEKLY,
    trusted_business_orders,
    trusted_completed_orders,
    trusted_silver,
    total_trusted_business_revenue,
)
from gold.gold_config import DEFAULT_HIGH_VALUE_THRESHOLD, GoldConfig  # noqa: E402
from silver.silver_foundation import apply_silver_all  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def main() -> None:
    data_dir = ROOT / "data"
    if not (data_dir / "orders.csv").exists():
        generate_all(seed=42, output_dir=str(data_dir))

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("gold-validation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bronze_config = BronzeConfig.from_env()
    bronze_config.input_dir = data_dir
    batch_id = "gold-report"
    ts = datetime.now(timezone.utc)
    bronze_dfs = {
        entity: prepare_bronze_dataframe(spark, entity, bronze_config, batch_id, ts)
        for entity in ("customers", "products", "orders")
    }
    silver_dfs = apply_silver_all(bronze_dfs)
    config = GoldConfig.from_env()
    gold = build_all_gold_dfs(silver_dfs, config)

    orders = silver_dfs["orders"]
    customers = silver_dfs["customers"]
    products = silver_dfs["products"]

    print("=== Gold validation (generated data, seed 42) ===")
    print(f"trusted PASS customers: {trusted_silver(customers).count()}")
    print(f"trusted PASS products: {trusted_silver(products).count()}")
    print(f"trusted PASS orders: {trusted_silver(orders).count()}")
    print(f"trusted Completed PASS orders: {trusted_completed_orders(orders).count()}")
    business_count = trusted_business_orders(orders, customers, products).count()
    print(
        f"trusted business orders (Gold realized revenue basis): {business_count}"
    )
    trusted_revenue = total_trusted_business_revenue(orders, customers, products)
    print(f"total trusted realized revenue: {trusted_revenue:.2f}")
    print(f"gold sales-by-product rows: {gold['sales_by_product'].count()}")
    print(f"gold revenue-by-customer rows: {gold['revenue_by_customer'].count()}")

    top10 = (
        gold["sales_by_product"]
        .orderBy(F.col("total_revenue").desc())
        .limit(10)
        .collect()
    )
    print("top 10 products by revenue:")
    for row in top10:
        print(
            f"  product_id={row.product_id} name={row.product_name} "
            f"revenue={float(row.total_revenue):.2f} orders={row.total_orders}"
        )

    seg = gold["customer_segmentation"].orderBy("segment_type").collect()
    print("customer segmentation (threshold=", config.high_value_threshold, "):")
    for row in seg:
        print(
            f"  {row.segment_type}: count={row.customer_count} "
            f"total_revenue={float(row.total_revenue):.2f} "
            f"avg_revenue={float(row.avg_revenue):.2f}"
        )

    daily = gold["daily_weekly_trends"].filter(F.col("period_type") == PERIOD_DAILY).count()
    weekly = gold["daily_weekly_trends"].filter(F.col("period_type") == PERIOD_WEEKLY).count()
    print(f"daily trend rows: {daily}")
    print(f"weekly trend rows: {weekly}")

    product_sum = float(
        gold["sales_by_product"].agg(F.sum("total_revenue")).collect()[0][0]
    )
    customer_sum = float(
        gold["revenue_by_customer"].agg(F.sum("total_revenue")).collect()[0][0]
    )
    print("reconciliation:")
    print(f"  product revenue sum: {product_sum:.2f}")
    print(f"  customer revenue sum: {customer_sum:.2f}")
    print(f"  trusted business-order revenue: {trusted_revenue:.2f}")
    print(f"  product == customer == trusted: {product_sum == customer_sum == trusted_revenue}")

    spark.stop()


if __name__ == "__main__":
    main()
