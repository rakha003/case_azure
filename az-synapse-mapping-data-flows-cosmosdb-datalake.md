# Scneario:
 Move Data from Source (Azure CosmosDB) and Transform Data using Azure Synapse to analyze

Sol:

Pipeline -> DataFlow Acitvity -> Data Sets(from both Source and Sink) -> Link the Services between Synapse with Source/Sink

In Synapse:
 - LINKED SERVICE WITH SYNAPSE -> INTEGRATION DATASETS -> DATAFLOW -> PIPELINE



# SOURCE
- AZURE COSMOS DB:
    Name of the Source Cosmos Database Account :  ssadcloudcosmosdb
    Name of the Database                       :  employeedb
    Name of the Container                      :  employeec


# SINK
- AZURE DATA LAKE STORAGE
    Name of the Data Lake           :       ssadcloudsynapsedl 
    Name of the Data Lake Container :        fzdata / outputs / output-from-cosmosdb
    Name of the Object              :       


# DESINATION
# AZURE SYNAPSE ANALYTICS


# LINKED SERVICES:
Name of Source Linked Service ( Azure Blob Storage) : LinkedService_CosmosDbNoSql
Name of Sink Linked Service ( Azure Data Lake Storage) : LinkedService_AzureDataLakeStorage

# INTEGRATION DATASETS:
Name of the Source DataSet: DataSet_Source_CosmosDbNoSql_Employee
Name of the Sink DataSet: DataSet_Sink_AzureDataLakeStorage_Employee

# MAPPING DATA FLOWS:
Name of the Mapping Data Flow:  Dataflow_Source_ComosDB_DataLake_Employee

