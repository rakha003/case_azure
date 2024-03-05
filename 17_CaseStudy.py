# Databricks notebook source
storage_account_name="ssadcloudsynapsedl"
storage_container_name="fzdata"
adventureworks_relative_path="/outputs/case-study-adventureworks/"
storage_sas_key="SAS_KEY"

# COMMAND ----------

# COMMAND ----------
print(storage_account_name)
print(storage_container_name)
print(adventureworks_relative_path)
print(storage_sas_key)

# COMMAND ----------

# COMMAND ----------
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  f"{storage_sas_key}"
)

# COMMAND ----------

adventureworks_abfss_path =f"abfss://%s@%s.dfs.core.windows.net/%s" %(storage_container_name,storage_account_name,adventureworks_relative_path)

# COMMAND ----------

print(adventureworks_abfss_path)

# COMMAND ----------

adventureworksDF = spark.read.parquet(adventureworks_abfss_path)

# COMMAND ----------

display(adventureworksDF)

# COMMAND ----------

adventureworksDF.createOrReplaceTempView("adventureworksView")

# COMMAND ----------

salesorderetailsbyProductQuery = "SELECT SUM(OrderQty) as TotalItemsSold, Color FROM adventureworksView GROUP BY Color"

# COMMAND ----------

salesorderetailsbyProductResults = spark.sql(salesorderetailsbyProductQuery)

# COMMAND ----------

display(salesorderetailsbyProductResults)
