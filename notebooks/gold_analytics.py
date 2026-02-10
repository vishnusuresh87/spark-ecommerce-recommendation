# GOLD ANALYTICS TABLES
# Create business-ready tables in the Gold layer

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
    max as spark_max,
    min as spark_min,
    date_format,
    to_date
)

print("CREATING GOLD ANALYTICS TABLES")

# Configure paths (DEV)

base_path = "/databricks"
silver_base = f"{base_path}/silver_dev"
gold_base = f"{base_path}/gold_dev"

orders_enriched_path = f"{silver_base}/orders_enriched"
products_enriched_path = f"{silver_base}/products_enriched"
customers_enriched_path = f"{silver_base}/customers_enriched"

product_metrics_path = f"{gold_base}/product_metrics"
customer_features_path = f"{gold_base}/customer_features"
daily_metrics_path = f"{gold_base}/daily_metrics"
time_series_metrics_path = f"{gold_base}/time_series_metrics"

print("Silver base:", silver_base)
print("Gold base  :", gold_base)

# Load Silver tables

print("\nLoading Silver tables...")

orders_enriched = spark.read.format("delta").load(orders_enriched_path)
products_enriched = spark.read.format("delta").load(products_enriched_path)
customers_enriched = spark.read.format("delta").load(customers_enriched_path)

print("orders_enriched rows:", orders_enriched.count())
print("products_enriched rows:", products_enriched.count())
print("customers_enriched rows:", customers_enriched.count())

# Ensure order_purchase_timestamp as date
orders_enriched = orders_enriched.withColumn(
    "order_date", to_date(col("order_purchase_timestamp"))
)

# Product-level metrics (Gold: product_metrics)

print("\nCreating product_metrics...")

product_metrics = (
    orders_enriched.groupBy("product_id")
    .agg(
        spark_sum(col("total_value")).alias("total_revenue"),
        spark_sum(col("freight_value")).alias("total_freight"),
        spark_count(col("order_id")).alias("total_orders"),
        spark_avg(col("review_score")).alias("avg_review_score"),
        spark_avg(col("price")).alias("avg_price"),
    )
)

# Join with product attributes from products_enriched
product_metrics = (
    product_metrics.alias("m")
    .join(
        products_enriched.select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "total_orders"  # if this in products_enriched
        ).alias("p"),
        on="product_id",
        how="left",
    )
    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        col("m.total_orders").alias("total_orders"),
        "total_revenue",
        "total_freight",
        "avg_review_score",
        "avg_price",
    )
)

product_metrics.write.format("delta").mode("overwrite").save(product_metrics_path)
print("product_metrics rows:", product_metrics.count())
print("Saved to:", product_metrics_path)

#Gold: customer_features

print("\nCreating customer_features...")

# We assume customers_enriched already contains:
#   lifetime_value, total_orders, avg_order_value
# Plus zip/city/state for segmentation

customer_features = customers_enriched.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "lifetime_value",
    "total_orders",
    "avg_order_value",
)

customer_features.write.format("delta").mode("overwrite").save(customer_features_path)
print("customer_features rows:", customer_features.count())
print("Saved to:", customer_features_path)

# Daily metrics

print("\nCreating daily_metrics...")

daily_metrics = (
    orders_enriched.groupBy("order_date")
    .agg(
        spark_count("order_id").alias("num_orders"),
        spark_sum("total_value").alias("total_revenue"),
        spark_sum("freight_value").alias("total_freight"),
        spark_avg("total_value").alias("avg_order_value"),
        spark_avg("review_score").alias("avg_review_score"),
    )
)

daily_metrics.write.format("delta").mode("overwrite").save(daily_metrics_path)
print("daily_metrics rows:", daily_metrics.count())
print("Saved to:", daily_metrics_path)

#Time-series metrics – Gold: time_series_metrics (year/month)

print("\nCreating time_series_metrics (year-month)...")

time_series_metrics = (
    daily_metrics
    .withColumn("year", date_format(col("order_date"), "yyyy"))
    .withColumn("month", date_format(col("order_date"), "MM"))
    .groupBy("year", "month")
    .agg(
        spark_sum("num_orders").alias("num_orders"),
        spark_sum("total_revenue").alias("total_revenue"),
        spark_sum("total_freight").alias("total_freight"),
        spark_avg("avg_order_value").alias("avg_order_value"),
        spark_avg("avg_review_score").alias("avg_review_score"),
    )
)

time_series_metrics.write.format("delta").mode("overwrite").save(time_series_metrics_path)
print("time_series_metrics rows:", time_series_metrics.count())
print("Saved to:", time_series_metrics_path)

#Summary

print("Gold analytics tables created")
