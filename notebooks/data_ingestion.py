"""
DATA INGESTION TO BRONZE LAYER
Loading all 9 Olist CSV files to Delta Bronze tables
"""

from pyspark.sql.types import *


#Define Olist schemas
print("DEFINING OLIST TABLE SCHEMAS")

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True),
])

REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), False),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

PRODUCT_CATEGORY_SCHEMA = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), True),
])

print("Schemas defined")


#Configure paths
source_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
bronze_base = "/databricks/bronze_dev"

print("LOADING CSV FILES TO BRONZE")


#Function to load CSV to Delta
def load_csv_to_bronze(csv_filename, table_name, schema):
    csv_path = f"{source_path}/{csv_filename}"
    bronze_path = f"{bronze_base}/{table_name}"
    
    try:
        # Read CSV
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .load(csv_path)
        
        row_count = df.count()
        
        # Write to Delta
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(bronze_path)
        
        print(f"✓ {table_name}: {row_count:,} rows")
        return True
        
    except Exception as e:
        print(f"✗ {table_name}: {str(e)}")
        return False


#Load all tables
tables = [
    ("olist_orders_dataset.csv", "orders", ORDERS_SCHEMA),
    ("olist_order_items_dataset.csv", "order_items", ORDER_ITEMS_SCHEMA),
    ("olist_customers_dataset.csv", "customers", CUSTOMERS_SCHEMA),
    ("olist_products_dataset.csv", "products", PRODUCTS_SCHEMA),
    ("olist_order_reviews_dataset.csv", "reviews", REVIEWS_SCHEMA),
    ("olist_order_payments_dataset.csv", "payments", PAYMENTS_SCHEMA),
    ("olist_sellers_dataset.csv", "sellers", SELLERS_SCHEMA),
    ("olist_geolocation_dataset.csv", "geolocation", GEOLOCATION_SCHEMA),
    ("product_category_name_translation.csv", "product_category", PRODUCT_CATEGORY_SCHEMA),
]

results = {}
for csv_file, table_name, schema in tables:
    results[table_name] = load_csv_to_bronze(csv_file, table_name, schema)


#Verify Bronze tables
print("VERIFYING BRONZE TABLES")

for table_name in [t[1] for t in tables]:
    try:
        bronze_path = f"{bronze_base}/{table_name}"
        df = spark.read.format("delta").load(bronze_path)
        row_count = df.count()
        col_count = len(df.columns)
        print(f"{table_name}: {row_count:,} rows, {col_count} columns")
    except Exception as e:
        print(f"{table_name}: Error - {e}")


#Summary
print("All 9 tables loaded to Bronze")
print("""
Bronze tables created:
  • orders: Raw order records
  • order_items: Items in each order
  • customers: Customer information
  • products: Product details
  • reviews: Customer reviews
  • payments: Payment information
  • sellers: Seller information
  • geolocation: Location mapping
  • product_category: Category translations

""")
