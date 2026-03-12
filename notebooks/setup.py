# Databricks notebook source
# DBTITLE 1,Cell 1
"""
SETUP & ENVIRONMENT CONFIGURATION
"""


# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import importlib
import builtins

# Add project root to path so config module is importable
project_root = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
sys.path.insert(0, project_root)

from config import paths
importlib.reload(paths)
from config.paths import DataPaths

# Get Spark session
spark = SparkSession.builder.appName("Olist-ETL").getOrCreate()


# Display environment info
print("SETUP: Olist E-Commerce Recommendation System")
print(f"\nSpark version: {spark.version}")
print(f"Python version: {sys.version}")
print(f"Databricks runtime: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")


print("CONFIGURING STORAGE PATHS")

paths = DataPaths("dev")

source_path = paths.source_path
bronze_path = paths.bronze_path
silver_path = paths.silver_path
gold_path = paths.gold_path
models_path = f'{paths.catalog}.{paths.model_schema}'

print(f"\nDev Environment:")
print(f"  Source: {source_path}")
print(f"  Bronze: {bronze_path}")
print(f"  Silver: {silver_path}")
print(f"  Gold: {gold_path}")
print(f"  Models: {models_path}")


#List uploaded CSV files
print("OLIST DATASET FILES")

files = dbutils.fs.ls(source_path)
csv_files = [f for f in files if f.name.endswith('.csv')]

print(f"\nFound {len(csv_files)} CSV files:")
for f in csv_files:
    size_mb = f.size / (1024 * 1024)
    print(f"{f.name}: {size_mb:.2f} MB")

total_size = builtins.sum(f.size for f in csv_files) / (1024 * 1024)
print(f"\nTotal size: {total_size:.2f} MB")


# Test Databricks sample dataset
print("TESTING DATABRICKS SAMPLE DATA")

#sample_df = spark.read.parquet("/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2013-08.parquet") # this is for aws workspaces
sample_df = spark.table("samples.nyctaxi.trips") # for gcp workspaces
sample_count = sample_df.count()

print(f"\nTest data loaded: {sample_count:,} records")
print(f"Schema: {sample_df.schema}")


#Create output messages
print("SETUP COMPLETE")
print("\nConfiguration saved to notebook variables")
