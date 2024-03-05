# Databricks notebook source
empDF = spark.read.csv('data/emp.csv',header=True,inferSchema=True)
#Note: data/emp.csv path not supported by DBFS

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.mkdir('/mydir')

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.csv","dbfs:/mydir/emp.csv")

# COMMAND ----------

dbutils.fs.ls('/mydir')

# COMMAND ----------

empDF = spark.read.csv('dbfs:/mydir/emp.csv',header=True,inferSchema=True)

# COMMAND ----------

display(empDF)

# COMMAND ----------

print(type(empDF))

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA LAKE OPERATIONS

# COMMAND ----------

#Step1: Read Data From Data Frame and store in 'Delta Table' Format
empDF.write.format("delta").save("/tmp/emp-delta-table")

# COMMAND ----------

#Step2: New DataFrame to Read Data from DeltaTable
empdeltaDF = spark.read.format("delta").load("/tmp/emp-delta-table")

# COMMAND ----------

display(empdeltaDF)

# COMMAND ----------

dbutils.fs.ls('/tmp/')

# COMMAND ----------

dbutils.fs.ls('/tmp/emp-delta-table')

# COMMAND ----------

display(dbutils.fs.ls('/tmp/emp-delta-table'))
