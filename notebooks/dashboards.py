"""
DASHBOARD QUERIES
======================================
Purpose:
    Loads Gold Delta tables and registers them as Spark temporary views,
    making them queryable via Databricks SQL dashboards. Path resolution
    is delegated to config/paths.py (single source of truth).

Usage:
    Run after ml_models.py, or independently once Gold tables exist.

Dependencies:
    - config/paths.py : DataPaths for environment-aware path resolution

Temp views created (queryable via Databricks SQL):
    - gold_product_metrics
    - gold_customer_segments
    - gold_daily_metrics
    - gold_time_series_metrics
    - gold_product_recommendations
"""

import sys
import os

# Add project root to path so config module is importable
project_root = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
sys.path.insert(0, project_root)

from config.paths import DataPaths


# Initialize paths
paths = DataPaths("dev")

print("CREATING DASHBOARD VIEWS")
print(f"  Gold base: {paths.gold_path}")


# Load Gold tables
gold_tables = {
    "gold_product_metrics"      : paths.get_gold_table("product_metrics"),
    "gold_customer_segments"    : paths.get_gold_table("customer_segments"),
    "gold_daily_metrics"        : paths.get_gold_table("daily_metrics"),
    "gold_time_series_metrics"  : paths.get_gold_table("time_series_metrics"),
    "gold_product_recommendations": paths.get_gold_table("product_recommendations"),
}

print("\nLoading Gold tables and registering temp views...")

for view_name, table_path in gold_tables.items():
    df = spark.read.format("delta").load(table_path)
    df.createOrReplaceTempView(view_name)
    print(f"  ✓ {view_name} ({df.count():,} rows)")


# Summary
print("\nDASHBOARD VIEWS READY")
print("""
Temp views available in Databricks SQL:
  • gold_product_metrics        : Revenue, orders, avg price per product
  • gold_customer_segments      : Customer cluster assignments
  • gold_daily_metrics          : Daily order and revenue aggregates
  • gold_time_series_metrics    : Monthly trend data
  • gold_product_recommendations: Top-10 product recommendations per customer
""")
