# Databricks notebook source
# MAGIC %md
# MAGIC #### Load CSV Datasets from Azure Data Lake Storage Gen2 Using SAS Token

# COMMAND ----------

storage_account_name="ssadclouddatalake"
storage_container_name="raw"
storge_relative_path="dep.csv"
storage_sas_token="<<SAS_TOKEN>>"

# COMMAND ----------

print(storage_account_name)
print(storage_container_name)
print(storge_relative_path)
print(storage_sas_token)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### 1.1 Using WASBS Method

# COMMAND ----------

spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net"%(storage_container_name,storage_account_name), storage_sas_token)

# COMMAND ----------

#Synatx:    wasb[s]://<containername>@<accountname>.blob.core.windows.net/<path>
wasbs_path ='wasbs://%s@%s.blob.core.windows.net/%s' %(storage_container_name,storage_account_name,storge_relative_path)

# COMMAND ----------

print(wasbs_path)

# COMMAND ----------

dbutils.fs.ls(wasbs_path)

# COMMAND ----------

#empDF = spark.read.csv(wasbs_path,header="True")
depDF = spark.read.csv(wasbs_path,header="True")

# COMMAND ----------

#empDF.show()
depDF.show()

# COMMAND ----------

empDF.createOrReplaceTempView('employeeView')

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

