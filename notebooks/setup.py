# Databricks notebook source

"""
SETUP & ENVIRONMENT CONFIGURATION
"""


# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

# Add project root to path so config module is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
models_path = paths.models_path

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

total_size = sum(f.size for f in csv_files) / (1024 * 1024)
print(f"\nTotal size: {total_size:.2f} MB")


# Test Databricks sample dataset
print("TESTING DATABRICKS SAMPLE DATA")

#sample_df = spark.read.parquet("/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2013-08.parquet") # this is for aws workspaces
sample_df = spark.read.parquet("samples.nyctaxi.trips") # for gcp workspaces
sample_count = sample_df.count()

print(f"\nTest data loaded: {sample_count:,} records")
print(f"Schema: {sample_df.schema}")


#Create output messages
print("SETUP COMPLETE")


#Save setup config to variables
spark.conf.set("olist.env", "dev")
spark.conf.set("olist.source_path", source_path)
spark.conf.set("olist.bronze_path", bronze_path)
spark.conf.set("olist.silver_path", silver_path)
spark.conf.set("olist.gold_path", gold_path)
spark.conf.set("olist.models_path", models_path)

print("\nConfiguration saved")
