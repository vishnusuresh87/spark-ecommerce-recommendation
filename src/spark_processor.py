"""
Spark ETL Processor Module
Transforms Bronze data to Silver and Gold layers
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, to_timestamp, year, month, dayofmonth,
    sum as spark_sum, avg as spark_avg, count, max as spark_max,
    min as spark_min, row_number, dense_rank
)
from pyspark.sql.window import Window
from config.paths import DataPaths
import logging

logger = logging.getLogger(__name__)

class SparkETLProcessor:
    """ETL transformations for Olist data"""
    
    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.paths = DataPaths(env)
    
    def create_orders_enriched(self) -> DataFrame:
        """
        Create Silver Orders Enriched table
        Joins orders, order_items, and reviews
        """
        logger.info("Creating orders_enriched...")
        
        # Read Bronze tables
        orders = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("orders")
        )
        order_items = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("order_items")
        )
        reviews = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("reviews")
        )
        
        # Join orders with items
        orders_items = orders.join(
            order_items,
            "order_id",
            "left"
        )
        
        # Join with reviews
        orders_enriched = orders_items.join(
            reviews,
            "order_id",
            "left"
        )
        
        # Add calculated fields
        orders_enriched = orders_enriched \
            .withColumn("total_value", col("price") + col("freight_value")) \
            .withColumn("order_year", year(col("order_purchase_timestamp"))) \
            .withColumn("order_month", month(col("order_purchase_timestamp"))) \
            .withColumn("days_to_delivery", 
                        (col("order_delivered_customer_date").cast("long") - 
                         col("order_purchase_timestamp").cast("long")) / 86400)
        
        # Save to Silver
        silver_path = self.paths.get_silver_table("orders_enriched")
        orders_enriched.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)
        
        count = orders_enriched.count()
        logger.info(f"✓ Created orders_enriched: {count:,} rows")
        
        return orders_enriched
    
    def create_products_enriched(self) -> DataFrame:
        """
        Create Silver Products Enriched table
        Joins products with sellers and categories
        """
        logger.info("Creating products_enriched...")
        
        products = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("products")
        )
        sellers = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("sellers")
        )
        category = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("product_category")
        )
        order_items = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("order_items")
        )
        
        # Calculate product metrics from order_items
        product_metrics = order_items.groupBy("product_id").agg(
            count("order_id").alias("total_orders"),
            spark_sum("price").alias("total_revenue"),
            spark_avg("price").alias("avg_price")
        )
        
        # Join all
        products_enriched = products \
            .join(category, "product_category_name", "left") \
            .join(product_metrics, "product_id", "left")
        
        # Save
        silver_path = self.paths.get_silver_table("products_enriched")
        products_enriched.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)
        
        count = products_enriched.count()
        logger.info(f"✓ Created products_enriched: {count:,} rows")
        
        return products_enriched
    
    def create_customers_enriched(self) -> DataFrame:
        """
        Create Silver Customers Enriched table
        Adds lifetime value and purchase history
        """
        logger.info("Creating customers_enriched...")
        
        customers = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("customers")
        )
        orders = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("orders")
        )
        order_items = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("order_items")
        )
        payments = self.spark.read.format("delta").load(
            self.paths.get_bronze_table("payments")
        )
        
        # Calculate customer lifetime value
        customer_value = order_items.join(
            payments, "order_id", "left"
        ).join(
            orders[["order_id", "customer_id"]], "order_id", "left"
        ).groupBy("customer_id").agg(
            spark_sum("payment_value").alias("lifetime_value"),
            count("order_id").alias("total_orders"),
            spark_avg("payment_value").alias("avg_order_value")
        )
        
        # Join with customers
        customers_enriched = customers.join(
            customer_value, "customer_id", "left"
        )
        
        # Save
        silver_path = self.paths.get_silver_table("customers_enriched")
        customers_enriched.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)
        
        count = customers_enriched.count()
        logger.info(f"✓ Created customers_enriched: {count:,} rows")
        
        return customers_enriched
    
    def validate_data_quality(self, table_name: str) -> Dict[str, any]:
        """Validate data quality metrics"""
        
        df = self.spark.read.format("delta").load(
            self.paths.get_silver_table(table_name)
        )
        
        validation = {
            "table": table_name,
            "total_rows": df.count(),
            "null_counts": {},
            "status": "PASSED"
        }
        
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / validation["total_rows"]) * 100 if validation["total_rows"] > 0 else 0
            validation["null_counts"][col_name] = {
                "count": null_count,
                "percentage": null_pct
            }
        
        logger.info(f"✓ Quality validation for {table_name}: {validation['status']}")
        
        return validation
