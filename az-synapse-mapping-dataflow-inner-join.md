=============================================================================
=============================================================================

Scenario:
Using Azure Synapse Mapping Dataflows, 
Build pipeline to handle "INNER JOIN" data from Source (Azure Blob Storage/raw) into Destination/Target(Azure Data Lake Storage/fzdata / outputs / merged-dataset)

Sol:

# SOURCE
AZURE BLOB STORAGE
- Name of the Azure Blob Storage                :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container      :  raw
- Name of the object 1                          :  emp.csv
- Name of the object 2                          :  dep.csv


# SINK
AZURE DATA LAKE STORAGE
- Name of the Azure Data Lake Storage            :  ssadcloudsynapsedl
- Name of the Azure Data Lake Storage Container  :  fzdata / outputs / merged-dataset
- Name of the Azure Data Lake Storage object     :  


# LINKED SERVICES
Name of the Linked Service (Source)              :  LinkedService_AzureBlobStorage
Name of the Linked Service (Sink)                :  LinkedService_AzureDataLakeStorage


# INTEGRATION DATA SETS(Data -> Linked Service -> Integration Data Sets)
Name of the DataSet(Source 1)                   :  DataSet_Source1_AzureBlobStorage_Employee
Name of the LinkedService(Source 1)             :  LinkedService_AzureBlobStorage

Name of the DataSet(Source 2)                   :  DataSet_Source2_AzureBlobStorage_Department
Name of the LinkedService(Source 2)             :  LinkedService_AzureBlobStorage

Name of the DataSet(Sink)                       :  DataSet_Sink_AzureDataLakeStorage_EmployeewithDepartment
Name of the LinkedService(Sink)                 :  LinkedService_AzureDataLakeStorage

# COPY ACTIVITY:
Name of the Pipeline: Pipeline_DataFlow_Employee_With_Department
