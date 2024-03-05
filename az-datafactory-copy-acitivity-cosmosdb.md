####################################################################################
Scenario - COPY Activity Using Azure Data Factory from Azure CosmosDB
####################################################################################

# Source: Azure CosmosDB Storage(Extract)
Name of the Azure CosmosDB Account      :       ssadcloudazcosmosdbforbookings
Name of the Azure CosmosDB Database     :       bookingsDB
Name of the Azure CosmosDB Container    :       cbookings


# Transform (Optional)


# Target: Azure Blob Storage(Load)
Name of the Blob Storage                :       ssadcloudblobstorage
Name of the Target Container            :       processed
Name of the object                      :       bookings.csv


# LINKED SERVICES

- Name of the Linked Service(Source) : LinkedService_CosmosDbNoSql

- Name of the Linked Service(Target) : LinkedService_AzureBlobStorage


# DATA SETS:

- Name of the DataSets(Source) : DataSet_Source_AzureCosmosDB_Bookings

- Name of the DataSets(Target) : DataSet_Target_AzBlobStorage_Bookings

# PIPELINE:
 Name of the Pipeline:          Pipeline_CP_FromSource_AzCosmosDB_ToTarget_Blob_Bookings
 Name of the Activity:          Activity_CP_FromSource_AzCosmosDB_ToTarget_Blob_Bookings