downloded the csv files  from kaggle "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
created the git repo in git hub spark-ecommerce-recommendation
cloned the repo to local
started documentation
added gitignore
started the databricks cluster
uploaded the csv files to the volume in catalog "dbfs:/Volumes/spark_8259559295155425/default/volume-alpha"
created the project structure
added the config structure with olist_schema and paths files
created the src structure with data_loader, spark_processor, feature_engineering, ml_models files
started creating the databricks notebooks files, added the setup.py


databricks path order dbfs>>Volumes>>catalog>>scheme(or database)>>volume(if created)>>tables
'dbfs:/Volumes/spark_8259559295155425/default/volume-alpha'

Root>>>catalog>>schema>>table,view,volume

