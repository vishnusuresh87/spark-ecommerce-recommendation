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
import mlflow
from mlflow.models import infer_signature
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
        logger.info(f"Training KMeans model with {n_clusters} clusters")
        
        # Load RFM features
        rfm = self.spark.table(
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
            seed=1899
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Fit model
        model = pipeline.fit(rfm)
        
        # Get predictions
        clusters = model.transform(rfm)
        
        # Create model signature for Unity Catalog
        input_example = rfm.select("recency", "frequency", "monetary").limit(5).toPandas()
        predictions_example = clusters.select("cluster").limit(5).toPandas()
        signature = infer_signature(input_example, predictions_example)
        
        # Register model in Unity Catalog using MLflow
        model_name = self.paths.get_model_name("kmeans_clustering")
        
        with mlflow.start_run(run_name="kmeans_clustering"):
            mlflow.log_param("n_clusters", n_clusters)
            mlflow.log_param("features", "recency,frequency,monetary")
            
            # Log and register model to Unity Catalog
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=model_name,
                dfs_tmpdir=self.paths.mlflow_tmp_path,
                signature=signature,
                input_example=input_example
            )
        
        # Save predictions
        gold_path = self.paths.get_gold_table("customer_segments")
        clusters.select("customer_id", "cluster").write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_path)
        
        logger.info(f"KMeans model registered in Unity Catalog: {model_name}")
        
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
        interactions = self.spark.table(
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
        
        # Create model signature for Unity Catalog
        input_example = indexed_data.select("customer_idx", "product_idx").limit(5).toPandas()
        
        # Register model in Unity Catalog using MLflow
        model_name = self.paths.get_model_name("als_recommendations")
        
        with mlflow.start_run(run_name="als_recommendations"):
            mlflow.log_param("rank", rank)
            mlflow.log_param("maxIter", max_iter)
            mlflow.log_param("regParam", reg_param)
            mlflow.log_param("coldStartStrategy", "drop")
            
            # Log and register model to Unity Catalog
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=model_name,
                dfs_tmpdir=self.paths.mlflow_tmp_path,
                input_example=input_example
            )
        
        # Generate recommendations
        recommendations = model.recommendForAllUsers(10)  # Top 10 per customer
        
        # Save
        gold_path = self.paths.get_gold_table("product_recommendations")
        recommendations.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_path)
        
        logger.info(f"ALS model registered in Unity Catalog: {model_name}")
        
        return model


