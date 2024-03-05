=============================================================================
=============================================================================

Scenario:
Using Azure Data Factory , Build pipeline to COPY data from Source (Azure CosmosDB) into
Destination/Target(Azure Blob Storage/processed)

Sol:

# SOURCE
AZURE COSMOSDB ACCOUNT
- Name of the Azure CosmosDB Account        : ssadcloud-azcosmosdbaccount
- Name of the Azure CosmosDB Database       : employeedb
- Name of the Azure CosmosDB Container      : employee


# TARGET
AZURE BLOB STORAGE
- Name of the Azure Blob Storage            :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container  :  processed
- Name of the Azure Blob Storage object     :  


# LINKED SERVICES
Name of the Linked Service (Source):  LinkedService_CosmosDbNoSql
Name of the Linked Service (Target):  LinkedService_AzureBlobStorage


# DATA SETS
Name of the DataSet(Source):  DataSet_Source_AzCosmosDB_Employee
Name of the LinkedService(Source):  LinkedService_AzureSqlDatabase

Name of the DataSet(Target):  DataSet_Target_AzBlobStorage_Employee2
Name of the LinkedService(Target):  LinkedService_AzureBlobStorage

# COPY ACTIVITY:
Name of the Pipeline: Pipeline_Copy_Source_AzSQLDB_Target_AzBlobStorage_Employee
Name of the Activity: Activity_Copy_Source_AzCosmosdb_Target_AzBlob_Emp

