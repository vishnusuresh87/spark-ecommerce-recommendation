"""
DATA INGESTION TO BRONZE LAYER
======================================
Purpose:
    Orchestrates the ingestion of all 9 Olist CSV files from the source volume
    into Bronze Delta tables. This notebook delegates all logic to the proper
    modules — schemas live in config/olist_schema.py, path resolution in
    config/paths.py, and ingestion logic in src/data_loader.py.

Usage:
    Run after setup.py.

Dependencies:
    - src/data_loader.py     : OlistDataLoader class (CSV → Bronze Delta)
    - config/paths.py        : DataPaths for environment-aware path resolution
    - config/olist_schema.py : OlistSchema for all 9 table schemas
"""

import sys
import os

# Add project root to path so src/config modules are importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data_loader import OlistDataLoader


# Initialize loader — internally uses config/paths.py and config/olist_schema.py
print("INITIALIZING DATA LOADER")
loader = OlistDataLoader(spark, env="dev")


# Load all 9 Olist CSV tables to Bronze Delta
print("\nLOADING CSV FILES TO BRONZE")
results = loader.load_all_olist_tables()


# Verify Bronze tables loaded correctly
print("\nVERIFYING BRONZE TABLES")
counts = loader.verify_bronze_tables()

for table_name, row_count in counts.items():
    print(f"  {table_name}: {row_count:,} rows")


# Summary
success_count = sum(1 for success, _ in results.values() if success)
print(f"\nDATA INGESTION COMPLETE: {success_count}/9 tables loaded to Bronze")
print("""
Bronze tables created:
  • orders          : Raw order records
  • order_items     : Items in each order
  • customers       : Customer information
  • products        : Product details
  • reviews         : Customer reviews
  • payments        : Payment information
  • sellers         : Seller information
  • geolocation     : Location mapping
  • product_category: Category translations
""")
