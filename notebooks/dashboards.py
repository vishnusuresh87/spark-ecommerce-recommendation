# DASHBOARD QUERIES
# to create views / tables for Databricks SQL dashboards.

print("CREATING DASHBOARD QUERIES")

base_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
gold_base = f"{base_path}/gold_dev"

product_metrics_path = f"{gold_base}/product_metrics"
customer_segments_path = f"{gold_base}/customer_segments"
daily_metrics_path = f"{gold_base}/daily_metrics"
time_series_metrics_path = f"{gold_base}/time_series_metrics"
product_recommendations_path = f"{gold_base}/product_recommendations"

product_metrics = spark.read.format("delta").load(product_metrics_path)
customer_segments = spark.read.format("delta").load(customer_segments_path)
daily_metrics = spark.read.format("delta").load(daily_metrics_path)
time_series_metrics = spark.read.format("delta").load(time_series_metrics_path)
product_recommendations = spark.read.format("delta").load(product_recommendations_path)

print("Loaded Gold tables for dashboarding.")

# Create temp views for SQL

product_metrics.createOrReplaceTempView("gold_product_metrics")
customer_segments.createOrReplaceTempView("gold_customer_segments")
daily_metrics.createOrReplaceTempView("gold_daily_metrics")
time_series_metrics.createOrReplaceTempView("gold_time_series_metrics")
product_recommendations.createOrReplaceTempView("gold_product_recommendations")

print("Temporary views created.")

