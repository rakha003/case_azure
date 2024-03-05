# Databricks notebook source
# MAGIC %md
# MAGIC #### Load Parquet Datasets from Azure Data Lake Storage Gen2 Using SAS Token

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Data Set 1 - Yellow Taxi DataSet
# MAGIC 1. Data Set 2 - Green Taxi DataSet

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

# MAGIC %md
# MAGIC #### Register DataFrame into a Temporary View

# COMMAND ----------

yellowtaxiDF.createOrReplaceTempView("yellowtaxiView")
greentaxiDF.createOrReplaceTempView("greentaxiView")

# COMMAND ----------

min_fareamountQuery = "SELECT min(fare_amount) as min_fareamount FROM yellowtaxiView"
max_fareamountQuery = "SELECT max(fare_amount) as max_fareamount FROM yellowtaxiView"
avg_fareamountQuery = "SELECT avg(fare_amount) as avg_fareamount FROM yellowtaxiView"

# COMMAND ----------

resultsforminfareAmount = spark.sql(min_fareamountQuery)
display(resultsforminfareAmount)


resultsformaxfareAmount = spark.sql(max_fareamountQuery)
display(resultsformaxfareAmount)

resultsforavgfareAmount = spark.sql(avg_fareamountQuery)
display(resultsforavgfareAmount)


