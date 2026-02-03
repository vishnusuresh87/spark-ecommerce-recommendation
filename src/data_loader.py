"""
Data Loader Module
Handles CSV ingestion from FileStore to Bronze layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from config.olist_schema import OlistSchema
from config.paths import DataPaths
from typing import Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OlistDataLoader:
    """Loads Olist CSV files into Bronze Delta tables"""
    
    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.paths = DataPaths(env)
        self.schema = OlistSchema
        
    def load_csv_to_bronze(
        self,
        csv_path: str,
        table_name: str,
        schema: StructType,
        mode: str = "overwrite"
    ) -> Tuple[bool, str]:
        """
        Load CSV file to Bronze Delta table
        
        Args:
            csv_path: Path to CSV file in FileStore
            table_name: Name of the table
            schema: Pyspark StructType schema
            mode: Write mode (overwrite, append, ignore, error)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            logger.info(f"Loading {table_name} from {csv_path}")
            
            # Read CSV
            df = self.spark.read \
                .format("csv") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(schema) \
                .load(csv_path)
            
            # Get row count
            row_count = df.count()
            logger.info(f"✓ Read {row_count:,} rows from {csv_path}")
            
            # Write to Bronze Delta
            bronze_path = self.paths.get_bronze_table(table_name)
            df.write \
                .format("delta") \
                .mode(mode) \
                .save(bronze_path)
            
            logger.info(f"✓ Wrote to Bronze: {bronze_path}")
            
            return True, f"Successfully loaded {row_count:,} rows to {table_name}"
            
        except Exception as e:
            error_msg = f"Error loading {table_name}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def load_all_olist_tables(self) -> Dict[str, Tuple[bool, str]]:
        """Load all 9 Olist tables from FileStore to Bronze"""
        
        results = {}
        
        tables_config = [
            ("orders", f"{self.paths.source_path}/olist_orders_dataset.csv", self.schema.ORDERS),
            ("order_items", f"{self.paths.source_path}/olist_order_items_dataset.csv", self.schema.ORDER_ITEMS),
            ("customers", f"{self.paths.source_path}/olist_customers_dataset.csv", self.schema.CUSTOMERS),
            ("products", f"{self.paths.source_path}/olist_products_dataset.csv", self.schema.PRODUCTS),
            ("reviews", f"{self.paths.source_path}/olist_order_reviews_dataset.csv", self.schema.REVIEWS),
            ("payments", f"{self.paths.source_path}/olist_order_payments_dataset.csv", self.schema.PAYMENTS),
            ("sellers", f"{self.paths.source_path}/olist_sellers_dataset.csv", self.schema.SELLERS),
            ("geolocation", f"{self.paths.source_path}/olist_geolocation_dataset.csv", self.schema.GEOLOCATION),
            ("product_category", f"{self.paths.source_path}/product_category_name_translation.csv", self.schema.PRODUCT_CATEGORY),
        ]
        
        for table_name, csv_path, schema in tables_config:
            success, message = self.load_csv_to_bronze(csv_path, table_name, schema)
            results[table_name] = (success, message)
            print(f"{'✓' if success else '✗'} {message}")
        
        return results
    
    def verify_bronze_tables(self) -> Dict[str, int]:
        """Verify all Bronze tables loaded correctly"""
        
        results = {}
        table_names = [
            "orders", "order_items", "customers", "products", 
            "reviews", "payments", "sellers", "geolocation", "product_category"
        ]
        
        for table_name in table_names:
            try:
                bronze_path = self.paths.get_bronze_table(table_name)
                df = self.spark.read.format("delta").load(bronze_path)
                row_count = df.count()
                results[table_name] = row_count
                logger.info(f"{table_name}: {row_count:,} rows")
            except Exception as e:
                logger.error(f"Error verifying {table_name}: {e}")
                results[table_name] = 0
        
        return results

