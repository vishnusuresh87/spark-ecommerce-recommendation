"""
GOLD ANALYTICS TABLES
======================================
Purpose:
    Orchestrates the Silver → Gold layer transformations for the Olist pipeline.
    Creates business-ready analytics tables aggregated from Silver Delta tables.
    Path resolution is delegated to config/paths.py.

Usage:
    Run after spark_etl.py.

Dependencies:
    - config/paths.py : DataPaths for environment-aware path resolution
    - Silver tables   : orders_enriched, products_enriched, customers_enriched

Gold tables produced:
    - product_metrics      : Revenue, orders, avg price and review score per product
    - customer_features    : Lifetime value, order count, avg order value per customer
    - daily_metrics        : Daily aggregated order and revenue metrics
    - time_series_metrics  : Monthly rollup of daily metrics for trend analysis
"""

import sys
import os
from pyspark.sql.functions import (
    col, to_date, date_format,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
)

# Add project root to path so config module is importable
project_root = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
sys.path.insert(0, project_root)

from config.paths import DataPaths


# Initialize paths
paths = DataPaths("dev")

print("CREATING GOLD ANALYTICS TABLES")
print(f"  Silver base : {paths.silver_path}")
print(f"  Gold base   : {paths.gold_path}")


# Load Silver tables
print("\nLoading Silver tables...")

orders_enriched    = spark.read.format("delta").load(paths.get_silver_table("orders_enriched"))
products_enriched  = spark.read.format("delta").load(paths.get_silver_table("products_enriched"))
customers_enriched = spark.read.format("delta").load(paths.get_silver_table("customers_enriched"))

print(f"  orders_enriched    : {orders_enriched.count():,} rows")
print(f"  products_enriched  : {products_enriched.count():,} rows")
print(f"  customers_enriched : {customers_enriched.count():,} rows")

# Add order_date column for daily aggregations
orders_enriched = orders_enriched.withColumn(
    "order_date", to_date(col("order_purchase_timestamp"))
)


# Gold: product_metrics
print("\nCreating product_metrics...")

product_metrics = (
    orders_enriched.groupBy("product_id")
    .agg(
        spark_sum("total_value").alias("total_revenue"),
        spark_sum("freight_value").alias("total_freight"),
        spark_count("order_id").alias("total_orders"),
        spark_avg("review_score").alias("avg_review_score"),
        spark_avg("price").alias("avg_price"),
    )
    .join(
        products_enriched.select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
        ),
        on="product_id",
        how="left",
    )
    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "total_orders",
        "total_revenue",
        "total_freight",
        "avg_review_score",
        "avg_price",
    )
)

product_metrics_path = paths.get_gold_table("product_metrics")
product_metrics.write.format("delta").mode("overwrite").save(product_metrics_path)
print(f"  product_metrics: {product_metrics.count():,} rows → {product_metrics_path}")


# Gold: customer_features
print("\nCreating customer_features...")

customer_features = customers_enriched.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "lifetime_value",
    "total_orders",
    "avg_order_value",
)

customer_features_path = paths.get_gold_table("customer_features")
customer_features.write.format("delta").mode("overwrite").save(customer_features_path)
print(f"  customer_features: {customer_features.count():,} rows → {customer_features_path}")


# Gold: daily_metrics
print("\nCreating daily_metrics...")

daily_metrics = (
    orders_enriched.groupBy("order_date")
    .agg(
        spark_count("order_id").alias("num_orders"),
        spark_sum("total_value").alias("total_revenue"),
        spark_sum("freight_value").alias("total_freight"),
        spark_avg("total_value").alias("avg_order_value"),
        spark_avg("review_score").alias("avg_review_score"),
    )
)

daily_metrics_path = paths.get_gold_table("daily_metrics")
daily_metrics.write.format("delta").mode("overwrite").save(daily_metrics_path)
print(f"  daily_metrics: {daily_metrics.count():,} rows → {daily_metrics_path}")


# Gold: time_series_metrics (monthly rollup)
print("\nCreating time_series_metrics...")

time_series_metrics = (
    daily_metrics
    .withColumn("year",  date_format(col("order_date"), "yyyy"))
    .withColumn("month", date_format(col("order_date"), "MM"))
    .groupBy("year", "month")
    .agg(
        spark_sum("num_orders").alias("num_orders"),
        spark_sum("total_revenue").alias("total_revenue"),
        spark_sum("total_freight").alias("total_freight"),
        spark_avg("avg_order_value").alias("avg_order_value"),
        spark_avg("avg_review_score").alias("avg_review_score"),
    )
)

time_series_path = paths.get_gold_table("time_series_metrics")
time_series_metrics.write.format("delta").mode("overwrite").save(time_series_path)
print(f"  time_series_metrics: {time_series_metrics.count():,} rows → {time_series_path}")


# Summary
print("\nGOLD ANALYTICS COMPLETE")
print("""
Gold tables created:
  • product_metrics     : Revenue, orders, avg price and review score per product
  • customer_features   : Lifetime value, order count, avg order value per customer
  • daily_metrics       : Daily aggregated order and revenue metrics
  • time_series_metrics : Monthly rollup for trend analysis
""")
