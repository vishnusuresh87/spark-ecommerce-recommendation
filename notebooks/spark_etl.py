"""
ETL TRANSFORMATION - BRONZE TO SILVER
======================================
Purpose:
    Orchestrates the Bronze → Silver layer transformations for the Olist pipeline.
    Reads from Bronze Delta tables, applies joins and enrichments, and writes
    cleaned Silver tables. Delegates all transformation logic to src/spark_processor.py.

Usage:
    Run after data_ingestion.py.

Dependencies:
    - src/spark_processor.py : SparkETLProcessor class (Bronze → Silver transformations)
    - config/paths.py        : DataPaths for environment-aware path resolution
"""

import sys
import os

# Add project root to path so src/config modules are importable
project_root = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
sys.path.insert(0, project_root)

from src.spark_processor import SparkETLProcessor
from pyspark.sql.functions import count, sum, avg



# Initialize ETL processor — internally uses config/paths.py
print("INITIALIZING ETL PROCESSOR")
processor = SparkETLProcessor(spark, env="dev")


# Bronze → Silver transformations
print("\nCREATING SILVER TABLES")

print("\nCreating orders_enriched...")
orders_enriched = processor.create_orders_enriched()
print(f"  orders_enriched: {orders_enriched.count():,} rows")

print("\nCreating products_enriched...")
products_enriched = processor.create_products_enriched()
print(f"  products_enriched: {products_enriched.count():,} rows")

print("\nCreating customers_enriched...")
customers_enriched, customer_level_enriched = processor.create_customers_enriched()
print(f"  customers_enriched: {customers_enriched.count():,} rows")
print(f"  customer_level_enriched: {customer_level_enriched.count():,} rows")


# Data Quality Validation
print("\nDATA QUALITY VALIDATION")

for table_name in ["orders_enriched", "products_enriched", "customers_enriched", "customer_level_enriched"]:
    result = processor.validate_data_quality(table_name)
    print(f"\n  {table_name}:")
    print(f"    Rows   : {result['total_rows']:,}")
    print(f"    Status : {result['status']}")


# Summary
print("\nSILVER LAYER COMPLETE")
print("""
Silver tables created:
  • orders_enriched   : Orders joined with items, reviews + delivery metrics
  • products_enriched : Products joined with categories + sales metrics
  • customers_enriched: Customers joined with lifetime value + order history
  • customer_level_enriched: Customer-level metrics by aggregating the customers_enriched table to summarize customer behavior,as customer_id in customers_enriched is non uniqe.
""")
