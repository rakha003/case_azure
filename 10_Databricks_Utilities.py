# Databricks notebook source
# MAGIC %md
# MAGIC ## What is Databricks Utility ?

# COMMAND ----------

# MAGIC %md
# MAGIC - A Library provided by Databricks  whcich provides a set of Commands and Functions 
# MAGIC - Databricks Enviorment and Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listing Supported Databricks Utilites
# MAGIC

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listing Supported Databricks Utilites For File System
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1. List of files and directories available in a Databricks File System(DBFS) path
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/nyctaxi-with-zipcodes/subsampled"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Directories in DBFS

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mnt/mydatadir/mydatasets/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/mydatadir/mydatasets/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 COPYING Data

# COMMAND ----------

dbutils.fs.cp("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled/part-00000-80b68cae-ce6a-41cf-87cd-2573d91b4c07-c000.snappy.parquet","dbfs:/mnt/mydatadir/mydatasets/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/mydatadir/mydatasets/")

# COMMAND ----------

taxiDF = spark.read.parquet("/mnt/mydatadir/mydatasets/part-00000-80b68cae-ce6a-41cf-87cd-2573d91b4c07-c000.snappy.parquet")

# COMMAND ----------

display(taxiDF)

# COMMAND ----------

# Avg Fare_amount
# Total Trip_distance
# Avg Fare Price/trip_distance
# max Fare, min Fare

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register DataFrame into a Temp View

# COMMAND ----------

taxiDF.createOrReplaceTempView('taxiView')

# COMMAND ----------

avg_FareAmountQuery = "SELECT avg(fare_amount) as AvgFareAmountforTrips FROM taxiView"

# COMMAND ----------

avg_FareAmountResults = spark.sql(avg_FareAmountQuery)

# COMMAND ----------

print(type(avg_FareAmountResults))

# COMMAND ----------

avg_FareAmountResults.show()

# COMMAND ----------

display(avg_FareAmountResults)

# COMMAND ----------

total_TripDistanceQuery = "SELECT sum(trip_distance) AS total_TripDistance FROM taxiView"

# COMMAND ----------

display(spark.sql(total_TripDistanceQuery))

# COMMAND ----------

AVGPricePerTripQuery = "SELECT SUM(fare_amount)/SUM(trip_distance) AS AVGPricePerTrip FROM taxiView"

# COMMAND ----------

display(spark.sql(AVGPricePerTripQuery))

# COMMAND ----------

display(spark.sql("SELECT MAX(fare_amount) FROM taxiView"))

# COMMAND ----------

display(spark.sql("SELECT MIN(fare_amount) FROM taxiView"))

# COMMAND ----------

outpath = dbutils.fs.ls("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.csv")

# COMMAND ----------

empDF = spark.read.csv("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.csv",header=True)

# COMMAND ----------

display(empDF)

# COMMAND ----------

empDF = spark.read.csv("data/emp.csv")

/#Workspace/Repos/iriscloudone@outlook.com/programming-with-pyspark/data/emp.csv

# COMMAND ----------

empDF = spark.read.csv("/Workspace/Repos/iriscloudone@outlook.com/programming-with-pyspark/data/emp.csv")


# COMMAND ----------

empDF = spark.read.csv("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.csv",header=True)

# COMMAND ----------

display(empDF)

# COMMAND ----------

empDF = spark.read.parquet("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.parquet")

# COMMAND ----------

display(empDF)

# COMMAND ----------

empDF = spark.read.parquet("https://ssadcloudblobstorage.blob.core.windows.net/raw/emp.parquet")

# COMMAND ----------


