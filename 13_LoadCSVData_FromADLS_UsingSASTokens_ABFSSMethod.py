# Databricks notebook source
# MAGIC %md
# MAGIC #### Load CSV Datasets from Azure Data Lake Storage Gen2 Using SAS Token

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Using ABFSS

# COMMAND ----------

storage_account_name="ssadclouddatalake"
storage_container_name="raw"
storge_relative_path="emp.csv"
storage_sas_key="<<SAS_KEY>>"

# COMMAND ----------

print(storage_account_name)
print(storage_container_name)
print(storge_relative_path)
print(storage_sas_key)

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  f"{storage_sas_key}"
)

# COMMAND ----------

#spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
abfss_path =f"abfss://%s@%s.dfs.core.windows.net/%s" %(storage_container_name,storage_account_name,storge_relative_path)

# COMMAND ----------

print(abfss_path)

# COMMAND ----------

dbutils.fs.ls(abfss_path)

# COMMAND ----------

empDF = spark.read.csv(abfss_path,header=True)

# COMMAND ----------

display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register DataFrame into a Temporary View

# COMMAND ----------

empDF.createOrReplaceTempView("employeeView")

# COMMAND ----------

min_salaryQuery = "SELECT min(emp_salary) as min_salary FROM employeeView"
max_salaryQuery = "SELECT max(emp_salary) as max_salary FROM employeeView"
avg_salaryQuery = "SELECT avg(emp_salary) as avg_salary FROM employeeView"

# COMMAND ----------

resultsforminSalary = spark.sql(min_salaryQuery)
display(resultsforminSalary)


resultsformaxSalary = spark.sql(max_salaryQuery)
display(resultsformaxSalary)


resultsforavgSalary = spark.sql(avg_salaryQuery)
display(resultsforavgSalary)

