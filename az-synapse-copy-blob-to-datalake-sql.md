=============================================================================
=============================================================================

Scenario:
Using Azure Synapse , Build pipeline to COPY data from Source (Azure Blob Storage) into
Destination/Target(Azure Data Lake) and handle Datawarehousing using SQL

Sol:

https://ssadcloudblobstorage.blob.core.windows.net/raw/emp.csv
# SOURCE
AZURE BLOB STORAGE
- Name of the Azure Blob Storage        : ssadcloudblobstorage
- Name of the Azure Blob Container      : raw
- Name of the Object                    : emp.csv


# SINK
AZURE DATA LAKE STORAGE
- Name of the Azure Data Lake            :  ssadcloudsynapsedl
- Name of the Azure Data Lake Container  :   fzdata / outputs / blob
- Name of the Object                     :  


# LINKED SERVICES
Name of the Linked Service (Source):  LinkedService_AzureBlobStorage
Name of the Linked Service (Sink):  LinkedService_AzureDataLakeStorage


# DATA SETS
Name of the DataSet(Source)         :  DataSet_Source_AzureBlobStorage_Employee
Name of the LinkedService(Source)   :  LinkedService_AzureBlobStorage

Name of the DataSet(Sink)           :  DataSet_Sink_AzureDataLake_Employee
Name of the LinkedService(Target)   :  LinkedService_AzureDataLakeStorage

# COPY ACTIVITY:
Name of the Pipeline: Pipeline_Copy_Source_AzBlob_Target_AzDataLakeStorage_Employee

