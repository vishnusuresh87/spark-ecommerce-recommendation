"""
Path Configuration for Dev, Test, and Production Environments
"""

import os
from enum import Enum


class Environment(Enum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


class DataPaths:
    """Configurable paths for different environments"""
    
    def __init__(self, env: str = "dev"):
        self.env = env.lower()
        self._set_paths()
    
    def _set_paths(self):
        """Set base paths based on environment"""
        
        if self.env == "dev":
            # YOUR ACTUAL CSV LOCATION FROM UNITY CATALOG
            self.base_path = "/databricks"
            self.source_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
            self.bronze_path = f"{self.base_path}/bronze_dev"
            self.silver_path = f"{self.base_path}/silver_dev"
            self.gold_path = f"{self.base_path}/gold_dev"
            self.models_path = f"{self.base_path}/models_dev"
            
        elif self.env == "test":
            # Test environment paths
            self.base_path = "/databricks"
            self.source_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
            self.bronze_path = f"{self.base_path}/bronze_test"
            self.silver_path = f"{self.base_path}/silver_test"
            self.gold_path = f"{self.base_path}/gold_test"
            self.models_path = f"{self.base_path}/models_test"
            
        else:  # prod
            # Production paths (can scale to 100GB+)
            self.base_path = "/mnt/data"
            self.source_path = "/Volumes/spark_8259559295155425/default/volume-alpha"
            self.bronze_path = f"{self.base_path}/bronze"
            self.silver_path = f"{self.base_path}/silver"
            self.gold_path = f"{self.base_path}/gold"
            self.models_path = f"{self.base_path}/models"
    
    def get_bronze_table(self, table_name: str) -> str:
        """Get Bronze layer path for a table"""
        return f"{self.bronze_path}/{table_name}"
    
    def get_silver_table(self, table_name: str) -> str:
        """Get Silver layer path for a table"""
        return f"{self.silver_path}/{table_name}"
    
    def get_gold_table(self, table_name: str) -> str:
        """Get Gold layer path for a table"""
        return f"{self.gold_path}/{table_name}"
    
    def get_model_path(self, model_name: str) -> str:
        """Get ML model path"""
        return f"{self.models_path}/{model_name}"


# Example usage
if __name__ == "__main__":
    # Dev environment
    dev_paths = DataPaths("dev")
    print(f"Source CSV location: {dev_paths.source_path}")
    print(f"Bronze path: {dev_paths.bronze_path}")
    print(f"Orders table: {dev_paths.get_bronze_table('orders')}")
    
    # Prod environment
    prod_paths = DataPaths("prod")
    print(f"Production bronze: {prod_paths.bronze_path}")
