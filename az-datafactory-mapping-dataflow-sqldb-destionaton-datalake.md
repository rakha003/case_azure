=============================================================================
=============================================================================

Scenario:
Using Azure Data Factory Mapping Data Flows , Build pipeline that handles the Data Integration involving
Data Sets - Employee and Department data from Source (Azure SQL Database) into
Destination/Target(Azure Data Lake Storage)

Sol:

# SOURCE
AZURE SQL DATABASE
- Name of the Azure SQL Database        : ssadcloudemployeedb
- Name of the Azure SQL Database Server : ssadcloudazsqlserver
- Name of Source Table1                 : Employee
- Name of Source Table2                 : Department



# TARGET(SINK)
AZURE DATA LAKE STORAGE
- Name of the Azure Data Lake Storage            :  ssadclouddatalake
- Name of the Azure Data Lake Storage Container  :   adf-output / output-from-sqldatabases
- Name of the Azure Data Lake Storage object     :  


# LINKED SERVICES
Name of the Linked Service (Source):  LinkedService_AzureSqlDatabase
Name of the Linked Service (Target):  LinkedService_AzureDataLakeStorage


# DATA SETS
Name of the DataSet1(Source):  DataSet_Source1_AzureSqlTable_Employee
Name of the LinkedService(Source):  LinkedService_AzureSqlDatabase

Name of the DataSet2(Source):  DataSet_Source2_AzureSqlTable_Department
Name of the LinkedService(Source):  LinkedService_AzureSqlDatabase

Name of the DataSet3(Target):  DataSet_Target_AzDataLakeStorage_EmployeewithDepartment
Name of the LinkedService(Target):  LinkedService_AzureDataLakeStorage

# MAPPING DATA FLOW:
Name of the Mapping Data Flow   :     DataFlow_SourceAzSQLDB_ToAzDataLake_Employee_With_Department

# MAPPING DATA FLOW ACTIVITY:
Name of the Pipeline            : Activity_DataFlow_SourceSQLDB_ToAzDataLake_Emp_With_Dep

# PIPELINE
Name of the Pipeline            : Pipeline_DataFlow_SourceAzSQLDB_ToAzDataLake_Employee_With_Department


