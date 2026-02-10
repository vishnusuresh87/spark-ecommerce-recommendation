# ML FEATURE ENGINEERING
# Create ML-ready feature tables in the Gold layer

from pyspark.sql.functions import (
    col,
    collect_list,
    explode,
    arrays_zip,
    lit,
    max as spark_max,
    sum as spark_sum,
    count as spark_count,
    avg as spark_avg,
    datediff,
    current_date
)
from pyspark.sql.window import Window

print("CREATING ML FEATURES")

# Configure paths

base_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
silver_base = f"{base_path}/silver_dev"
gold_base = f"{base_path}/gold_dev"

orders_enriched_path = f"{silver_base}/orders_enriched"
customer_rfm_path = f"{gold_base}/customer_rfm_features"
customer_product_interactions_path = f"{gold_base}/customer_product_interactions"
product_cooccurrence_path = f"{gold_base}/product_cooccurrence"

print("Silver:", silver_base)
print("Gold  :", gold_base)

#Load Silver table

orders_enriched = spark.read.format("delta").load(orders_enriched_path)
print("orders_enriched rows:", orders_enriched.count())

# Ensure order_date exists
orders_enriched = orders_enriched.withColumn(
    "order_date", col("order_purchase_timestamp").cast("date")
)

#Customer–product interactions (ALS input)

print("\nCreating customer_product_interactions...")

interactions = (
    orders_enriched
    .select(
        "customer_id",
        "product_id",
        col("total_value").alias("interaction_weight"),
        col("review_score").alias("rating"),
    )
    .filter(col("customer_id").isNotNull() & col("product_id").isNotNull())
)

# Fill null ratings with neutral 3
interactions = interactions.fillna({"rating": 3.0})

interactions.write.format("delta").mode("overwrite").save(
    customer_product_interactions_path
)
print("customer_product_interactions rows:", interactions.count())
print("Saved to:", customer_product_interactions_path)

#Product co-occurrence matrix (for “bought together”)

print("\nCreating product_cooccurrence...")

# Aggregate product list per order
products_per_order = (
    orders_enriched.groupBy("order_id")
    .agg(collect_list("product_id").alias("product_list"))
)

# Build pairs (p1, p2) from product_list
# For simplicity we keep (product_id) and the full list;
# in a real project we may explode into pair rows.
product_pairs = products_per_order.select(
    "order_id",
    explode("product_list").alias("product_id")
)

product_pairs.write.format("delta").mode("overwrite").save(
    product_cooccurrence_path
)
print("product_cooccurrence rows:", product_pairs.count())
print("Saved to:", product_cooccurrence_path)

#Customer RFM features

print("\nCreating customer_rfm_features...")

# Reference date = max order_date in dataset
ref_date_row = orders_enriched.select(spark_max("order_date").alias("ref_date")).collect()[0]
ref_date = ref_date_row["ref_date"] or current_date()

rfm = (
    orders_enriched.groupBy("customer_id")
    .agg(
        spark_max("order_date").alias("last_order_date"),
        spark_count("order_id").alias("frequency"),
        spark_sum("total_value").alias("monetary"),
    )
    .withColumn("recency", datediff(lit(ref_date), col("last_order_date")))
)

# average order value
rfm = rfm.withColumn("avg_order_value", col("monetary") / col("frequency"))

rfm.write.format("delta").mode("overwrite").save(customer_rfm_path)
print("customer_rfm_features rows:", rfm.count())
print("Saved to:", customer_rfm_path)

# Summary

print("ML feature tables created")
