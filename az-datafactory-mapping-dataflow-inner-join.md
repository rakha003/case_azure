=============================================================================
=============================================================================

Scenario:
Using Azure Data Factory Mapping Dataflows, 
Build pipeline to handle "INNER JOIN" data from Source (Azure Blob Storage/raw) into Destination/Target(Azure Blob Storage/processed)

Sol:

# SOURCE
AZURE BLOB STORAGE
- Name of the Azure Blob Storage            :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container  :  raw
- Name of the object 1                      :  emp.csv
- Name of the object 2                      :  dep.csv


# TARGET
AZURE BLOB STORAGE
- Name of the Azure Blob Storage            :  ssadcloudblobstorage
- Name of the Azure Blob Storage Container  :  processed
- Name of the Azure Blob Storage object     :  


# LINKED SERVICES
Name of the Linked Service (Source):  LinkedService_AzureBlobStorage
Name of the Linked Service (Target):  LinkedService_AzureBlobStorage


# DATA SETS
Name of the DataSet(Source 1)               :  DataSet_Source1_AzureBlobStorage_Employee
Name of the LinkedService(Source 1)         :  LinkedService_AzureBlobStorage

Name of the DataSet(Source 2)               :  DataSet_Source2_AzureBlobStorage_Department
Name of the LinkedService(Source 2)         :  LinkedService_AzureBlobStorage

Name of the DataSet(Target)                 :  DataSet_Target_AzureBlobStorage_EmployeewithDepartment
Name of the LinkedService(Target)           :  LinkedService_AzureBlobStorage

# COPY ACTIVITY:
Name of the Pipeline: Pipeline_DataFlow_Employee_With_Department
