=============================================================================
=============================================================================

Scenario:
Using Azure Data Factory Mapping Dataflows, 
Build pipeline to filter Movies Data Set with data from Source (Azure Blob Storage/raw) into Destination/Target(Azure Blob Storage/processed)

Sol:

# SOURCE
AZURE SQL DATABASE
- Name of the Azure Blob Storage            :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container  :  processed
- Name of the object                        :  moviesDB.csv


# TARGET
AZURE BLOB STORAGE
- Name of the Azure Blob Storage            :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container  :  processed
- Name of the Azure Blob Storage object     :  


# LINKED SERVICES
Name of the Linked Service (Source):  LinkedService_AzureBlobStorage
Name of the Linked Service (Target):  LinkedService_AzureBlobStorage


# DATA SETS
Name of the DataSet(Source)                 :  DataSet_Source_AzureSqlTable_Movies
Name of the LinkedService(Source)           :  LinkedService_AzureBlobStorage

Name of the DataSet(Target)                 :  DataSet_Target_AzureSqlTable_Movies
Name of the LinkedService(Target)           :  LinkedService_AzureBlobStorage

# COPY ACTIVITY:
Name of the Pipeline: Pipeline_DataFlow_Movies_RatingGreaterthan7
