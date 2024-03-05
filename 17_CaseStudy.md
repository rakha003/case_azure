=============================================================================
=============================================================================

Scenario:
Using Azure Data Factory , Build pipeline to COPY data from Source (Azure SQL Database) into
Destination/Target(Azure Blob Storage/processed)

Sol:

# SOURCE
AZURE SQL DATABASE
- Name of the Azure SQL Database        : adventureworkssalesddb
- Name of the Azure SQL Database Server : adventureworksmainserver


# SINK
AZURE DATA LAKE STORAGE
- Name of the Azure Data Lake Storage            :  ssadcloudsynapsedl
- Name of the Azure Data Lake Container          :  fzdata/outputs/case-study-adventureworks
- Name of the Azure Data Lake object             :  

# TARGET
AZURE DATABRICKS
- Name of the Databricks Workspace:                   ssadcloud-main-ws
- Name of the Databricks Compute Cluster:             main-compute-cluster
- Name of the Databricks Datawarehouse:               main-datawarehouse


# LINKED SERVICES
Name of the Linked Service (Source):  LS_AzureSqlDatabase
Name of the Linked Service (Sink):  LinkedService_AzureBlobStorage


# DATA SETS
Name of the DataSet(Source1):  DataSet_Source1_AzureSqlTable_SalesOrder
Name of the DataSet(Source2):  DataSet_Source2_AzureSqlTable_Products


Name of the DataSet(Sink):  DataSet_Sink_AzureDataLakeStorage_SalesOrderswithProducts
Name of the LinkedService(Target):  LinkedService_AzureBlobStorage

# DATA FLOW: 
Name of the DataFlow : DataFlowCaseStudyAdventureWorks
# COPY ACTIVITY:
Name of the Pipeline: Pipeline_Copy_Source_AzSQLDB_Target_AzBlobStorage_Employee

