#MODEL TRAINING (KMEANS + ALS)

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

print("TRAINING ML MODELS")

#Configure paths

base_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
gold_base = f"{base_path}/gold_dev"
models_base = f"{base_path}/models_dev"

customer_rfm_path = f"{gold_base}/customer_rfm_features"
customer_segments_path = f"{gold_base}/customer_segments"
customer_product_interactions_path = f"{gold_base}/customer_product_interactions"
product_recommendations_path = f"{gold_base}/product_recommendations"

kmeans_model_path = f"{models_base}/kmeans_customer_segments"
als_model_path = f"{models_base}/als_recommendations"

print("Gold  :", gold_base)
print("Models:", models_base)

#Load feature tables

rfm = spark.read.format("delta").load(customer_rfm_path).fillna(0)
interactions = spark.read.format("delta").load(customer_product_interactions_path)

print("customer_rfm_features rows:", rfm.count())
print("interactions rows:", interactions.count())

#Train KMeans clustering on RFM

print("\nTraining KMeans clustering model...")

assembler = VectorAssembler(
    inputCols=["recency", "frequency", "monetary"],
    outputCol="features"
)
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)
kmeans = KMeans(
    k=5,
    seed=1899,
    featuresCol="scaled_features",
    predictionCol="cluster"
)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])

kmeans_model = pipeline.fit(rfm)
clusters = kmeans_model.transform(rfm)

# Save model & segments
kmeans_model.write().overwrite().save(kmeans_model_path)
clusters.select("customer_id", "cluster").write.format("delta").mode("overwrite").save(
    customer_segments_path
)

print("KMeans model saved to:", kmeans_model_path)
print("customer_segments rows:", clusters.count())
print("Saved clusters to:", customer_segments_path)

#Train ALS recommendation model

print("\nTraining ALS recommendation model...")

# Convert string IDs to numeric indices
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

indexed = customer_indexer.fit(interactions).transform(interactions)
indexed = product_indexer.fit(indexed).transform(indexed)

als = ALS(
    rank=10,
    maxIter=10,
    regParam=0.01,
    userCol="customer_idx",
    itemCol="product_idx",
    ratingCol="rating",
    coldStartStrategy="drop"
)

als_model = als.fit(indexed)

# Save ALS model
als_model.write().overwrite().save(als_model_path)
print("ALS model saved to:", als_model_path)

# Generate top-10 recommendations per customer
recommendations = als_model.recommendForAllUsers(10)

# Keep customer_idx and exploded recommendation list
from pyspark.sql.functions import explode

recs_exploded = (
    recommendations
    .withColumn("rec", explode("recommendations"))
    .select(
        "customer_idx",
        col("rec.itemId").alias("product_idx"),
        col("rec.rating").alias("predicted_rating")
    )
)

recs_exploded.write.format("delta").mode("overwrite").save(product_recommendations_path)
print("product_recommendations rows:", recs_exploded.count())
print("Saved recommendations to:", product_recommendations_path)

#Summary

print("KMeans + ALS models trained and saved")
