# Databricks notebook source
# MAGIC %md
# MAGIC #### Load Parquet Datasets from Azure Data Lake Storage Gen2 Using SAS Token into SQL Tables /Databases

# COMMAND ----------

# MAGIC %md
# MAGIC Important Pages : https://learn.microsoft.com/en-us/azure/databricks/ingestion/copy-into/

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Data Set 1 - Yellow Taxi DataSet
# MAGIC 1. Data Set 2 - Green Taxi DataSet

# COMMAND ----------

# https://ssadclouddatalake.blob.core.windows.net/raw/yellow_tripdata_2023-01.parquet
# https://ssadclouddatalake.blob.core.windows.net/raw/green_tripdata_2023-01.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Using ABFSS

# COMMAND ----------

storage_account_name="ssadclouddatalake"

storage_container_name="raw"

yellowtaxi_storge_relative_path="yellow_tripdata_2023-01.parquet"

greentaxi_storge_relative_path="green_tripdata_2023-01.parquet"

storage_sas_key="H2UCXyjFH6ysr4tevckQQWro2i+btcedUATM4K3l/aCZv/gcN4V8fA8wt/iU8I6QvBS5G+304Mf6+AStuo9XNQ=="

# COMMAND ----------

print(storage_account_name)
print(storage_container_name)
print(yellowtaxi_storge_relative_path)
print(greentaxi_storge_relative_path)
print(storage_sas_key)

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  f"{storage_sas_key}"
)

# COMMAND ----------

#spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
yellowtaxi_abfss_path =f"abfss://%s@%s.dfs.core.windows.net/%s" %(storage_container_name,storage_account_name,yellowtaxi_storge_relative_path)

# COMMAND ----------

greentaxi_abfss_path =f"abfss://%s@%s.dfs.core.windows.net/%s" %(storage_container_name,storage_account_name,greentaxi_storge_relative_path)

# COMMAND ----------

print(yellowtaxi_abfss_path)
print(greentaxi_abfss_path)

# COMMAND ----------

dbutils.fs.ls(yellowtaxi_abfss_path)
dbutils.fs.ls(greentaxi_abfss_path)

# COMMAND ----------

yellowtaxiDF = spark.read.parquet(yellowtaxi_abfss_path)
greentaxiDF = spark.read.parquet(greentaxi_abfss_path)

# COMMAND ----------

display(yellowtaxiDF)
display(greentaxiDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS yellowtaxitable;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS greentaxitable;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE yellowtaxitable();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE greentaxitable();

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE yellowtaxitable SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE greentaxitable SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO yellowtaxitable FROM 'abfss://raw@ssadclouddatalake.dfs.core.windows.net/yellow_tripdata_2023-01.parquet' FILEFORMAT=PARQUET 
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO greentaxitable FROM 'abfss://raw@ssadclouddatalake.dfs.core.windows.net/green_tripdata_2023-01.parquet' FILEFORMAT=PARQUET 
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(fare_amount) as MinFare,max(fare_amount) as MaxFare ,avg(fare_amount) as AvgFare  FROM yellowtaxitable;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(fare_amount) as MinFare,max(fare_amount) as MaxFare ,avg(fare_amount) as AvgFare  FROM greentaxitable;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(fare_amount),passenger_count FROM greentaxitable GROUP BY passenger_count
