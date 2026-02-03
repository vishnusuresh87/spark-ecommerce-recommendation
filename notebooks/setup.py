# Databricks notebook source

"""
SETUP & ENVIRONMENT CONFIGURATION
"""


# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

# Get Spark session
spark = SparkSession.builder.appName("Olist-ETL").getOrCreate()


# Display environment info
print("SETUP: Olist E-Commerce Recommendation System")
print(f"\nSpark version: {spark.version}")
print(f"Python version: {sys.version}")
print(f"Databricks runtime: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")

#Create file storage paths
print("CONFIGURING STORAGE PATHS")

base_path = "/databricks"
source_path = "/Volumes/spark_8259559295155425/default/volume-alpha"

# Create layer paths
bronze_path = f"{base_path}/bronze_dev"
silver_path = f"{base_path}/silver_dev"
gold_path = f"{base_path}/gold_dev"
models_path = f"{base_path}/models_dev"

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

print("\nConfiguration saved")
