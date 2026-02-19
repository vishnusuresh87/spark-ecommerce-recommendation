"""
ML FEATURE ENGINEERING - GOLD LAYER
======================================
Purpose:
    Orchestrates the creation of ML-ready feature tables in the Gold layer.
    Reads from Silver Delta tables and produces feature tables used by the
    ML model training notebook. Delegates all logic to src/feature_engineering.py.

Usage:
    Run after spark_etl.py and before ml_models.py.

Dependencies:
    - src/feature_engineering.py : FeatureEngineer class (Silver → Gold ML features)
    - config/paths.py            : DataPaths for environment-aware path resolution

Gold tables produced:
    - customer_product_interactions : Customer-product interaction matrix (ALS input)
    - product_cooccurrence          : Products bought together per order
    - customer_rfm_features         : Recency, Frequency, Monetary features per customer
"""

import sys
import os

# Add project root to path so src/config modules are importable
project_root = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
sys.path.insert(0, project_root)

from src.feature_engineering import FeatureEngineer


# Initialize feature engineer — internally uses config/paths.py
print("CREATING ML FEATURES")
engineer = FeatureEngineer(spark, env="dev")


# Customer-product interaction matrix (ALS input)
print("\nCreating customer_product_interactions...")
interactions = engineer.create_customer_product_interactions()
print(f"  customer_product_interactions: {interactions.count():,} rows")


# Product co-occurrence matrix ("bought together")
print("\nCreating product_cooccurrence...")
cooccurrence = engineer.create_product_cooccurrence_matrix()
print(f"  product_cooccurrence: {cooccurrence.count():,} rows")


# Customer RFM features (KMeans input)
print("\nCreating customer_rfm_features...")
rfm = engineer.create_rfm_features()
print(f"  customer_rfm_features: {rfm.count():,} rows")


# Summary
print("\nML FEATURE ENGINEERING COMPLETE")
print("""
Gold ML feature tables created:
  • customer_product_interactions : Customer-product matrix for ALS model
  • product_cooccurrence          : Products bought together per order
  • customer_rfm_features         : Recency, Frequency, Monetary per customer
""")
