=============================================================================
=============================================================================

Scenario:
Using Azure Data Factory , Build pipeline to COPY data from Source (Azure SQL Database) into
Destination/Target(Azure Blob Storage/processed)

Sol:

# SOURCE
AZURE SQL DATABASE
- Name of the Azure SQL Database        : ssadcloudemployeedb
- Name of the Azure SQL Database Server : ssadcloudazsqlserver


# TARGET
AZURE BLOB STORAGE
- Name of the Azure Blob Storage            :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container  :  processed
- Name of the Azure Blob Storage object     :  


# LINKED SERVICES
Name of the Linked Service (Source):  LinkedService_AzureSqlDatabase
Name of the Linked Service (Target):  LinkedService_AzureBlobStorage


# DATA SETS
Name of the DataSet(Source):  DataSet_Source_AzureSqlTable_Employee
Name of the LinkedService(Source):  LinkedService_AzureSqlDatabase

Name of the DataSet(Target):  DataSet_Target_AzBlobStorage_Employee
Name of the LinkedService(Target):  LinkedService_AzureBlobStorage

# COPY ACTIVITY:
Name of the Pipeline: Pipeline_Copy_Source_AzSQLDB_Target_AzBlobStorage_Employee

