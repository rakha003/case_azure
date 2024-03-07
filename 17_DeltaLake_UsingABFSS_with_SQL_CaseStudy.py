# Databricks notebook source
# MAGIC %md
# MAGIC #### Load Parquet Datasets from Azure Data Lake Storage Gen2 Using SAS Token into SQL Tables /Databases

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Data Set  - Case Study
# MAGIC https://ssadclouddatalake.blob.core.windows.net/output-for-casestudy/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Using ABFSS

# COMMAND ----------

storage_account_name="ssadclouddatalake"

storage_container_name="output-for-casestudy"

casestudy_storge_relative_path="/"

storage_sas_key="H2UCXyjFH6ysr4tevckQQWro2i+btcedUATM4K3l/aCZv/gcN4V8fA8wt/iU8I6QvBS5G+304Mf6+AStuo9XNQ=="

# COMMAND ----------

print(storage_account_name)
print(storage_container_name)
print(casestudy_storge_relative_path)
print(storage_sas_key)

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  f"{storage_sas_key}"
)

# COMMAND ----------

#spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
casestudy_abfss_path =f"abfss://%s@%s.dfs.core.windows.net/%s" %(storage_container_name,storage_account_name,casestudy_storge_relative_path)

# COMMAND ----------

print(casestudy_abfss_path)

# COMMAND ----------

dbutils.fs.ls(casestudy_abfss_path)

# COMMAND ----------

casestudyDF = spark.read.parquet(casestudy_abfss_path)

# COMMAND ----------

display(casestudyDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS casestudytable;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE casestudytable();

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE casestudytable SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO casestudytable FROM 'abfss://output-for-casestudy@ssadclouddatalake.dfs.core.windows.net//' FILEFORMAT=PARQUET 
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(OrderQty) as ItemsSold,Color FROM casestudytable GROUP BY Color;
