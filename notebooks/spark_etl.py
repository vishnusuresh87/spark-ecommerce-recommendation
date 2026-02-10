"""
ETL TRANSFORMATION - BRONZE TO SILVER
Join and clean data
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window


bronze_base = "/databricks/bronze_dev"
silver_base = "/databricks/silver_dev"


# Create Orders Enriched
print("\nCreating orders_enriched...")

orders = spark.read.format("delta").load(f"{bronze_base}/orders")
order_items = spark.read.format("delta").load(f"{bronze_base}/order_items")
reviews = spark.read.format("delta").load(f"{bronze_base}/reviews")

orders_enriched = orders.join(
    order_items, "order_id", "left"
).join(
    reviews, "order_id", "left"
).withColumn(
    "total_value", col("price") + col("freight_value")
).withColumn(
    "order_year", year(col("order_purchase_timestamp"))
).withColumn(
    "order_month", month(col("order_purchase_timestamp"))
).withColumn(
    "days_to_delivery",
    (col("order_delivered_customer_date").cast("long") - 
     col("order_purchase_timestamp").cast("long")) / 86400
)

orders_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base}/orders_enriched")

print(f"Orders enriched: {orders_enriched.count():,} rows")


#Create Products Enriched
print("\nCreating products_enriched...")

products = spark.read.format("delta").load(f"{bronze_base}/products")
sellers = spark.read.format("delta").load(f"{bronze_base}/sellers")
category = spark.read.format("delta").load(f"{bronze_base}/product_category")

# Product metrics
product_metrics = order_items.groupBy("product_id").agg(
    count("order_id").alias("total_orders"),
    sum("price").alias("total_revenue"),
    avg("price").alias("avg_price")
)

products_enriched = products \
    .join(category, "product_category_name", "left") \
    .join(product_metrics, "product_id", "left") \
    .fillna(0)

products_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base}/products_enriched")

print(f"Products enriched: {products_enriched.count():,} rows")


# Create Customers Enriched
print("\nCreating customers_enriched...")

customers = spark.read.format("delta").load(f"{bronze_base}/customers")
payments = spark.read.format("delta").load(f"{bronze_base}/payments")

# Customer value
customer_value = orders.join(
    order_items, "order_id", "left"
).join(
    payments, "order_id", "left"
).groupBy("customer_id").agg(
    sum("payment_value").alias("lifetime_value"),
    count("order_id").alias("total_orders"),
    avg("payment_value").alias("avg_order_value")
)

customers_enriched = customers.join(
    customer_value, "customer_id", "left"
).fillna(0)

customers_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base}/customers_enriched")

print(f"Customers enriched: {customers_enriched.count():,} rows")


# Data Quality Checks
print("DATA QUALITY VALIDATION")

for table_name in ["orders_enriched", "products_enriched", "customers_enriched"]:
    df = spark.read.format("delta").load(f"{silver_base}/{table_name}")
    
    print(f"\n{table_name}:")
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    
    # Check for nulls
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print(f"  Total nulls: {sum(null_counts.collect()[0])}")


#Summary
print("Silver layer created")
