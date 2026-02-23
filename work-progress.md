downloded the csv files  from kaggle "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
created the git repo in git hub spark-ecommerce-recommendation
cloned the repo to local
started documentation
added gitignore
started the databricks cluster
uploaded the csv files to the volume in catalog "dbfs:/Volumes/spark_8259559295155425/default/volume-alpha"  >>> is going to be the current source
created the project structure
added the config structure with olist_schema and paths files
created the src structure with data_loader, spark_processor, feature_engineering, ml_models files
started creating the databricks notebooks files, added the setup.py


databricks path order dbfs>>Volumes>>catalog>>scheme(or database)>>volume(if created)>>tables
'dbfs:/Volumes/spark_8259559295155425/default/volume-alpha'   ----> this is the initial source path

Root>>>catalog>>schema>>table,view,volume


### File Organization

```
spark-ecommerce-recommendation/
├── config/
│   ├── __init__.py
│   ├── olist_schema.py          # Table schemas & field definitions
│   └── paths.py                 # Path configuration (dev/prod)
│
├── src/
│   ├── __init__.py
│   ├── data_loader.py           # CSV ingestion logic
│   ├── spark_processor.py       # ETL transformations
│   ├── feature_engineering.py   # ML feature creation
│   └── ml_models.py             # Clustering & recommendation models
│
├── notebooks/
│   ├── setup.py              # Environment setup
│   ├── data_ingestion.py     # Bronze layer load
│   ├── spark_etl.py          # Silver layer transform
│   ├── gold_analytics.py     # Gold layer aggregations
│   ├── gold_ml_features.py   # ML feature engineering
│   ├── ml_models.py          # Model training
│   └── dashboards.py         # Query creation
│
├── tests/
│   ├── __init__.py
│   └── test_quality.py          # Data quality validation tests
│
├── docs/
│   ├── README.md
│   ├── ARCHITECTURE.md
│   └── DEPLOYMENT.md
│
├── .gitignore
├── requirements.txt
└── setup.py
```

olist_schema.py ---->> 

1. Defining the schema with StructType([StructField()])
2. Defining the rules of the field as dictionary


paths.py

1. Defining the paths for different enironments, ie dev, test, prod
Change it based on the cloud locations and databricks catalogs

data_loader.py

1. defining the function for loading the raw(csv) to bronze as delta tables
2. defining a function to list all the loaded tables
3. defining a function for verifying the bronze layer tables.

spark_processor.py

1. read bronze table data(delta)
2. function to create orders_ enriched by combining the orders, order_items, reviews and creating order_enriched field with selected columns order_total, order_year, order_month, days_to_delivery and save to silver
3. function to create products_enriched by joining the products, sellers, category, order_items, and calculated the product metrics with aggregate functions as save to silver
4. function to create customers_enriched by combining customers, orders, order_items, payments, calculate customer value, and saving to silver
5. function to validate the data quality

feature_engineering.py

1. Defines the functions for transforming silver layer to ML ready feature tables in gold layer
2. functions to create customer product interaction matrix, which is an input to the ALS model
3. function to create product co occurance matrix
4. function to produce RFM features

ml_models.py (src)

1. Definition for creating recommendation models with K-means clustering models, and ALS model.


setup.py

1. defining the setup for different enironments, ie dev, test, prod

data_ingestion.py

1. load all the csv files to bronze layer
2. verify the bronze layer tables

spark_etl.py

1. read bronze table data(delta)
2. create orders_ enriched by combining the orders, order_items, reviews and creating order_enriched field with selected columns order_total, order_year, order_month, days_to_delivery and save to silver
3. create products_enriched by joining the products, sellers, category, oeder_items, and calculated the product metrics with aggregate functions as save to silver
4. create customers_enriched by combining customers, orders, order_items, payments, calculate customer value, and saving to silver
5. validating the data quality

gold_ml_features.py

1. implementing the functions defined in feature_engineering.py
2. The newly created features product_coocuurance matrix, customer product matrix, RFM features in gold layer.

ml_models.py (notebooks)

1. implementing the functions defined in ml_models.py in src.

















