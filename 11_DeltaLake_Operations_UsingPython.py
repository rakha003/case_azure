# Databricks notebook source
# MAGIC %md
# MAGIC ## What is Delta Lake ?
# MAGIC -  Open Source Storage Layer
# MAGIC -  Brings in ACID compliance to Apache Spark and Big Data work loads
# MAGIC
# MAGIC ## How to use Delta Lake
# MAGIC - Delta Table
# MAGIC - ACID Transactions
# MAGIC - Data Versioning
# MAGIC - Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is Delta Lake ?
# MAGIC - Delta Lake is an open source project that enables building a Lakehouse architecture on top of data lakes 
# MAGIC - Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on top of existing data lakes, such as S3, ADLS, GCS, and HDFS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Specifically, Delta Lake offers:
# MAGIC - **ACID transactions on Spark**: Serializable isolation levels ensure that readers never see inconsistent data
# MAGIC - **Scalable metadata handling**: Leverages Spark distributed processing power to handle all the metadata for petabyte-scale tables with billions of files at ease
# MAGIC - **Streaming and batch unification**: A table in Delta Lake is a batch table as well as a streaming source and sink.Streaming data ingest, batch historic backfill, interactive queries all just work out of the box.
# MAGIC - **Schema enforcement**: Automatically handles schema variations to prevent insertion of bad records during ingestion
# MAGIC - **Time travel**: Data versioning enables rollbacks, full historical audit trails, and reproducible machine learning experiments
# MAGIC - **Upserts and deletes**: Supports merge, update and delete operations to enable complex use cases like change-data-capture, slowly-changing-dimension (SCD) operations, streaming upserts, and so on

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Upload the Source Data to Mount Point

# COMMAND ----------

#Mount Point to Store Source DataSets
dbutils.fs.mkdirs("/mnt/mydatasets/")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/"))

# COMMAND ----------

# Upload the Source DataSet to Created Mount Point
dbutils.fs.cp("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/yellow_tripdata_2023_01.parquet","dbfs:/mnt/mydatasets/")

# COMMAND ----------

#List the DataSet from Created Mount Point
dbutils.fs.ls("dbfs:/mnt/mydatasets/")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/mydatasets/yellow_tripdata_2023_01.parquet"))

# COMMAND ----------

# DataFrames to read Data from DataSets that are uploaded to created Mount Point
taxidata_path = "dbfs:/mnt/mydatasets/yellow_tripdata_2023_01.parquet"

# COMMAND ----------

# Create DataFrame to Read the Data
yellowtaxiDF = spark.read.parquet(taxidata_path)
display(yellowtaxiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Delta Tables

# COMMAND ----------

#yellowtaxiDF.write.format("delta").mode("overwrite").save("/yellowtaxideltaTable")
yellowtaxiDF.write.format("delta").save("/yellowtaxideltaTable")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/yellowtaxideltaTable"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read from Delta Tables

# COMMAND ----------

newyellowtaxiDF = spark.read.format("delta").load("/yellowtaxideltaTable")
display(newyellowtaxiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Update to Delta Tables

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 4.1 Conditional update without overwrite

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

yellowtaxiDF.write.format("delta").mode("overwrite").save("/yellowtaxideltaTable")

# COMMAND ----------

display(yellowtaxiDF)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, "/yellowtaxideltaTable")

# COMMAND ----------

deltaTable.update(condition="PULocationID = '161'",set={"PULocationID":"'162'"})

# COMMAND ----------

updatedyellowtaxiDF= spark.read.format("delta").load("/yellowtaxideltaTable")
display(updatedyellowtaxiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time Travel

# COMMAND ----------

history_yellowtaxiDFV0 = spark.read.format("delta").option("versionAsOf",0).load("/yellowtaxideltaTable")

# COMMAND ----------

display(history_yellowtaxiDFV0)

# COMMAND ----------

history_yellowtaxiDFV5 = spark.read.format("delta").option("versionAsOf",5).load("/yellowtaxideltaTable")

# COMMAND ----------

display(history_yellowtaxiDFV1)

# COMMAND ----------

display(dbutils.fs.rm('/yellowtaxideltaTable/**'))

# COMMAND ----------

display(dbutils.fs.rm('dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp-2.csv'))

# COMMAND ----------

dbutils.fs.help()
