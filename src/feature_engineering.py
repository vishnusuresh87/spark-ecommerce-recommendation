"""
Feature Engineering Module
Creates ML-ready features for recommendation models
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, row_number, collect_list, lit,
    datediff, current_date, months_between
)
from pyspark.sql.window import Window
from pyspark.mllib.feature import HashingTF, IDF
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
        Products bought together by customers
        """
        logger.info("Creating product co-occurrence matrix...")
        
        orders_enriched = self.spark.read.format("delta").load(
            self.paths.get_silver_table("orders_enriched")
        )
        
        # Get products purchased together
        products_per_order = orders_enriched.groupBy("order_id").agg(
            collect_list("product_id").alias("product_list")
        )
        
        # Explode and create pairs
        from pyspark.sql.functions import explode, arrays_zip
        
        pairs = products_per_order.select(
            explode(col("product_list")).alias("product_1")
        )
        
        # Save as feature table
        gold_path = self.paths.get_gold_table("product_cooccurrence")
        pairs.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        logger.info(f"✓ Created product co-occurrence matrix")
        return pairs
    
    def create_customer_product_interactions(self) -> DataFrame:
        """
        Create customer-product interaction matrix
        For ALS model training
        """
        logger.info("Creating customer-product interactions...")
        
        orders_enriched = self.spark.read.format("delta").load(
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
            .save(gold_path)
        
        count = interactions.count()
        logger.info(f"✓ Created {count:,} customer-product interactions")
        
        return interactions
    
    def create_rfm_features(self) -> DataFrame:
        """
        Create RFM (Recency, Frequency, Monetary) features
        Recency: Days since last order
        Frequency: Number of orders
        Monetary: Total spent
        """
        logger.info("Creating RFM features...")
        
        orders_enriched = self.spark.read.format("delta").load(
            self.paths.get_silver_table("orders_enriched")
        )
        
        # RFM calculations
        from pyspark.sql.functions import max as spark_max, datediff, current_date
        
        reference_date = self.spark.sql("SELECT current_date()").collect()[0][0]
        
        rfm = orders_enriched.groupBy("customer_id").agg(
            datediff(lit(reference_date), spark_max("order_purchase_timestamp")).alias("recency"),
            col("customer_id").count().alias("frequency"),
            spark_sum("total_value").alias("monetary")
        )
        
        # Save
        gold_path = self.paths.get_gold_table("customer_rfm_features")
        rfm.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        logger.info(f"✓ Created RFM features for customers")
        
        return rfm


