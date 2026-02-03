"""
ML Models Module
Trains clustering and recommendation models
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
from config.paths import DataPaths
import logging

logger = logging.getLogger(__name__)

class RecommendationModels:
    """Train ML models for recommendations"""
    
    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.paths = DataPaths(env)
    
    def train_kmeans_clustering(self, n_clusters: int = 5) -> object:
        """
        Train KMeans clustering on customer RFM features
        Segments customers into clusters
        """
        logger.info(f"Training KMeans model with {n_clusters} clusters...")
        
        # Load RFM features
        rfm = self.spark.read.format("delta").load(
            self.paths.get_gold_table("customer_rfm_features")
        ).fillna(0)
        
        # Feature engineering
        assembler = VectorAssembler(
            inputCols=["recency", "frequency", "monetary"],
            outputCol="features"
        )
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features"
        )
        
        kmeans = KMeans(
            k=n_clusters,
            featuresCol="scaled_features",
            predictionCol="cluster",
            seed=42
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Fit model
        model = pipeline.fit(rfm)
        
        # Get predictions
        clusters = model.transform(rfm)
        
        # Save model
        model_path = self.paths.get_model_path("kmeans_clustering")
        model.write().overwrite().save(model_path)
        
        # Save predictions
        gold_path = self.paths.get_gold_table("customer_segments")
        clusters.select("customer_id", "cluster").write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        logger.info(f"✓ KMeans model trained and saved")
        
        return model
    
    def train_als_recommendations(
        self,
        rank: int = 10,
        max_iter: int = 10,
        reg_param: float = 0.01
    ) -> object:
        """
        Train ALS (Alternating Least Squares) model
        For product recommendations
        """
        logger.info("Training ALS recommendation model...")
        
        # Load interactions
        interactions = self.spark.read.format("delta").load(
            self.paths.get_gold_table("customer_product_interactions")
        )
        
        # Convert customer and product IDs to numeric
        from pyspark.ml.feature import StringIndexer
        
        customer_indexer = StringIndexer(
            inputCol="customer_id",
            outputCol="customer_idx",
            handleInvalid="skip"
        )
        
        product_indexer = StringIndexer(
            inputCol="product_id",
            outputCol="product_idx",
            handleInvalid="skip"
        )
        
        indexed_data = customer_indexer.fit(interactions).transform(interactions)
        indexed_data = product_indexer.fit(indexed_data).transform(indexed_data)
        
        # Train ALS
        als = ALS(
            rank=rank,
            maxIter=max_iter,
            regParam=reg_param,
            userCol="customer_idx",
            itemCol="product_idx",
            ratingCol="rating",
            coldStartStrategy="drop"
        )
        
        model = als.fit(indexed_data)
        
        # Save model
        model_path = self.paths.get_model_path("als_recommendations")
        model.write().overwrite().save(model_path)
        
        # Generate recommendations
        recommendations = model.recommendForAllUsers(10)  # Top 10 per customer
        
        # Save
        gold_path = self.paths.get_gold_table("product_recommendations")
        recommendations.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        logger.info(f"✓ ALS model trained and saved")
        
        return model


