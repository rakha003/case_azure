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

# https://ssadclouddatalake.blob.core.windows.net/raw/emp.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Using ABFSS

# COMMAND ----------

storage_account_name="ssadclouddatalake"

storage_container_name="raw"

employee_storge_relative_path="emp.csv"

storage_sas_key="H2UCXyjFH6ysr4tevckQQWro2i+btcedUATM4K3l/aCZv/gcN4V8fA8wt/iU8I6QvBS5G+304Mf6+AStuo9XNQ=="

# COMMAND ----------

print(storage_account_name)
print(storage_container_name)
print(employee_storge_relative_path)
print(storage_sas_key)

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  f"{storage_sas_key}"
)

# COMMAND ----------

#spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
employee_storge_relative_path_abfss_path =f"abfss://%s@%s.dfs.core.windows.net/%s" %(storage_container_name,storage_account_name,employee_storge_relative_path)

# COMMAND ----------

print(employee_storge_relative_path_abfss_path)

# COMMAND ----------

dbutils.fs.ls(employee_storge_relative_path_abfss_path)

# COMMAND ----------

empDF = spark.read.csv(employee_storge_relative_path_abfss_path,header=True,inferSchema=True)

# COMMAND ----------

display(empDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS employeeTable;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employeeTable();

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employeeTable SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO employeeTable FROM 'abfss://raw@ssadclouddatalake.dfs.core.windows.net/emp.csv' FILEFORMAT = CSV 
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                   'header' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employeeTable;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(emp_salary) as MinSalary,max(emp_salary) as MaxSalary ,avg(emp_salary) as AvgSalary  FROM employeeTable;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(emp_salary) as AvgSalarybyDep,emp_department FROM employeeTable GROUP BY emp_department;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(fare_amount),passenger_count FROM greentaxitable GROUP BY passenger_count
