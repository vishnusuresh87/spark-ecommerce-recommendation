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
            # Development environment paths - using Volumes for compatibility
            self.base_path = "Medallion"
            self.source_path = "/Volumes/spark_all_purpose/default/input_sample_volume"
            self.bronze_path = f"{self.base_path}.bronze_dev"
            self.silver_path = f"{self.base_path}.silver_dev"
            self.gold_path = f"{self.base_path}.gold_dev"
            # Models registered in Unity Catalog Model Registry
            self.catalog = "Medallion"
            self.model_schema = "models_dev"
            # MLflow temporary directory for Spark ML models (required for UC)
            self.mlflow_tmp_path = "/Volumes/spark_all_purpose/default/mlflow_tmp"
         
        elif self.env == "test":
            # Test environment paths
            self.base_path = "Medallion"
            self.source_path = "/Volumes/spark_all_purpose/default/input_sample_volume"
            self.bronze_path = f"{self.base_path}.bronze_test"
            self.silver_path = f"{self.base_path}.silver_test"
            self.gold_path = f"{self.base_path}.gold_test"
            # Models registered in Unity Catalog Model Registry
            self.catalog = "Medallion"
            self.model_schema = "models_test"
            # MLflow temporary directory for Spark ML models (required for UC)
            self.mlflow_tmp_path = "/Volumes/spark_all_purpose/default/mlflow_tmp"
            
        else:  # prod
            # Production paths (can scale to 100GB+)
            self.base_path = "Medallion"
            self.source_path = "/Volumes/spark_all_purpose/default/input_sample_volume"
            self.bronze_path = f"{self.base_path}/bronze"
            self.silver_path = f"{self.base_path}/silver"
            self.gold_path = f"{self.base_path}/gold"
            # Models registered in Unity Catalog Model Registry
            self.catalog = "Medallion"
            self.model_schema = "models_prod"
            # MLflow temporary directory for Spark ML models (required for UC)
            self.mlflow_tmp_path = "/Volumes/spark_all_purpose/default/mlflow_tmp"
    
    def get_bronze_table(self, table_name: str) -> str:
        """Get Bronze layer path for a table"""
        return f"{self.bronze_path}.{table_name}"
    
    def get_silver_table(self, table_name: str) -> str:
        """Get Silver layer path for a table"""
        return f"{self.silver_path}.{table_name}"
    
    def get_gold_table(self, table_name: str) -> str:
        """Get Gold layer path for a table"""
        return f"{self.gold_path}.{table_name}"
    
    def get_model_name(self, model_name: str) -> str:
        """Get Unity Catalog registered model name (catalog.schema.model)"""
        return f"{self.catalog}.{self.model_schema}.{model_name}"


# Example usage
if __name__ == "__main__":
    # Dev environment
    dev_paths = DataPaths("dev")
    print(f"Source CSV location: {dev_paths.source_path}")
    print(f"Bronze path: {dev_paths.bronze_path}")
    print(f"Orders table: {dev_paths.get_bronze_table('orders')}")
    print(f"Model name: {dev_paths.get_model_name('kmeans_clustering')}")
    
    # Prod environment
    prod_paths = DataPaths("prod")
    print(f"Production bronze: {prod_paths.bronze_path}")
    print(f"Production models: {prod_paths.get_model_name('als_recommendations')}")
