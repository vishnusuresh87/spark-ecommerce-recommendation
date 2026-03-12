"""
MODEL TRAINING - KMEANS + ALS
======================================
Purpose:
    Orchestrates training of KMeans customer segmentation and ALS product
    recommendation models. Reads Gold feature tables and saves trained models
    and prediction outputs. Delegates all ML logic to src/ml_models.py.

Usage:
    Run after gold_ml_features.py.

Dependencies:
    - src/ml_models.py : RecommendationModels class (KMeans + ALS training)
    - config/paths.py  : DataPaths for environment-aware path resolution

Outputs:
    - Models saved  : models/kmeans_clustering, models/als_recommendations
    - Gold tables   : customer_segments, product_recommendations
"""

import sys
import os

# Add project root to path so src/config modules are importable
project_root = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
sys.path.insert(0, project_root)

from src.ml_models import RecommendationModels


# Initialize model trainer — internally uses config/paths.py
print("TRAINING ML MODELS")
models = RecommendationModels(spark, env="dev")


# Train KMeans clustering on customer RFM features
print("\nTraining KMeans clustering model (k=5)...")
kmeans_model = models.train_kmeans_clustering(n_clusters=5)
print("  KMeans model trained and saved ")


# Train ALS recommendation model
print("\nTraining ALS recommendation model...")
als_model = models.train_als_recommendations(rank=10, max_iter=10, reg_param=0.01)
print("  ALS model trained and saved ✓")


# Summary
print("\nMODEL TRAINING COMPLETE")
print("""
Models trained and saved:
  • kmeans_clustering   : Customer segmentation (5 clusters) from RFM features
  • als_recommendations : Product recommendations (top-10 per customer)

Gold tables produced:
  • customer_segments       : Customer ID → cluster assignment
  • product_recommendations : Customer ID → top-10 recommended products
""")
