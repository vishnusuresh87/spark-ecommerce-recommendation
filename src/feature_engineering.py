"""
Feature Engineering Module
Creates ML-ready features for recommendation models
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, collect_list, lit, explode, array_distinct,
    datediff, current_date, size,
    sum as spark_sum, max as spark_max, count
)
from config.paths import DataPaths
import logging

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Create ML features from Silver data"""
    
    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.paths = DataPaths(env)
    
    def create_product_cooccurrence_matrix(self) -> DataFrame:
        """
        Create product co-occurrence matrix
        Products bought together by customers in the same order
        """
        logger.info("Creating product co-occurrence matrix...")
        
        orders_enriched = self.spark.table(
            self.paths.get_silver_table("orders_enriched")
        )
        
        # Get distinct products per order (handle duplicates)
        products_per_order = orders_enriched.groupBy("order_id").agg(
            array_distinct(collect_list("product_id")).alias("product_list")
        ).filter(size("product_list") > 1)  # Only orders with 2+ products
        
        # Create product pairs by exploding twice
        # First explosion creates product_1
        exploded_1 = products_per_order.select(
            col("order_id"),
            explode("product_list").alias("product_1"),
            col("product_list")
        )
        
        # Second explosion creates product_2
        exploded_2 = exploded_1.select(
            col("order_id"),
            col("product_1"),
            explode("product_list").alias("product_2")
        )
        
        # Filter out self-pairs and create co-occurrence counts
        cooccurrence = exploded_2 \
            .filter(col("product_1") < col("product_2")) \
            .groupBy("product_1", "product_2") \
            .agg(count("order_id").alias("cooccurrence_count")) \
            .orderBy(col("cooccurrence_count").desc())
        
        # Save as feature table
        gold_path = self.paths.get_gold_table("product_cooccurrence")
        cooccurrence.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(gold_path)
        
        row_count = cooccurrence.count()
        logger.info(f"Created product co-occurrence matrix with {row_count:,} product pairs")
        return cooccurrence
    
    def create_customer_product_interactions(self) -> DataFrame:
        """
        Create customer-product interaction matrix
        For ALS model training
        """
        logger.info("Creating customer-product interactions...")
        
        orders_enriched = self.spark.table(
            self.paths.get_silver_table("orders_enriched")
        )
        
        # Create interaction scores
        interactions = orders_enriched.select(
            col("customer_id"),
            col("product_id"),
            col("total_value").alias("interaction_weight"),
            col("review_score").alias("rating")
        ).filter(
            col("customer_id").isNotNull() & col("product_id").isNotNull()
        )
        
        # Fill nulls (ratings)
        interactions = interactions.fillna({"rating": 3})
        
        # Save
        gold_path = self.paths.get_gold_table("customer_product_interactions")
        interactions.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(gold_path)
        
        row_count = interactions.count()
        logger.info(f"Created {row_count:,} customer-product interactions")
        
        return interactions
    
    def create_rfm_features(self) -> DataFrame:
        """
        Create RFM (Recency, Frequency, Monetary) features
        Recency: Days since last order
        Frequency: Number of orders
        Monetary: Total spent
        """
        logger.info("Creating RFM features")
        
        orders_enriched = self.spark.table(
            self.paths.get_silver_table("orders_enriched")
        )
        
        # RFM calculations
        reference_date = self.spark.sql("SELECT current_date()").collect()[0][0]
        
        rfm = orders_enriched.groupBy("customer_id").agg(
            datediff(lit(reference_date), spark_max("order_purchase_timestamp")).alias("recency"),
            count("order_id").alias("frequency"),
            spark_sum("total_value").alias("monetary")
        )
        
        # Save
        gold_path = self.paths.get_gold_table("customer_rfm_features")
        rfm.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(gold_path)
        
        logger.info(f"Created RFM features for customers")
        
        return rfm


