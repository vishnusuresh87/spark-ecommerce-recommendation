"""
Olist Dataset Schema Definition
Complete field definitions and data types for all 9 tables
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType, LongType, BooleanType
)

class OlistSchema:
    """Complete schema for all Olist tables"""
    
    # Orders Table
    ORDERS = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),  # delivered, shipped, etc
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
    ])
    
    # Order Items Table
    ORDER_ITEMS = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
    ])
    
    #Customers Table
    CUSTOMERS = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ])
    
    #Products Table
    PRODUCTS = StructType([
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
    
    # Reviews Table
    REVIEWS = StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("review_score", IntegerType(), True),  # 1-5
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True),
    ])
    
    # Payments Table
    PAYMENTS = StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), False),
        StructField("payment_type", StringType(), True),  # credit_card, boleto, etc
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ])
    
    # Sellers Table
    SELLERS = StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
    ])
    
    # Geolocation Table
    GEOLOCATION = StructType([
        StructField("geolocation_zip_code_prefix", StringType(), False),
        StructField("geolocation_lat", DoubleType(), True),
        StructField("geolocation_lng", DoubleType(), True),
        StructField("geolocation_city", StringType(), True),
        StructField("geolocation_state", StringType(), True),
    ])
    
    # Product Category Translation
    PRODUCT_CATEGORY = StructType([
        StructField("product_category_name", StringType(), False),
        StructField("product_category_name_english", StringType(), True),
    ])

class DataQualityRules:
    """Data quality validation rules"""
    
    RULES = {
        'orders': [
            ("order_id NOT NULL", "Order ID cannot be null"),
            ("customer_id NOT NULL", "Customer ID cannot be null"),
            ("order_purchase_timestamp NOT NULL", "Purchase timestamp required"),
            ("order_status IN ('delivered', 'shipped', 'processing', 'canceled')", "Invalid order status"),
        ],
        'order_items': [
            ("order_id NOT NULL", "Order ID cannot be null"),
            ("product_id NOT NULL", "Product ID cannot be null"),
            ("price > 0", "Price must be positive"),
            ("freight_value >= 0", "Freight value cannot be negative"),
        ],
        'customers': [
            ("customer_id NOT NULL", "Customer ID cannot be null"),
            ("customer_city NOT NULL", "Customer city required"),
        ],
        'products': [
            ("product_id NOT NULL", "Product ID cannot be null"),
            ("product_category_name NOT NULL", "Product category required"),
        ],
        'reviews': [
            ("order_id NOT NULL", "Order ID cannot be null"),
            ("review_score BETWEEN 1 AND 5", "Review score must be 1-5"),
        ],
    }
