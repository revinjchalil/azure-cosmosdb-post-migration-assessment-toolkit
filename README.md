# Cosmos DB post-migration assessment toolkit

This toolkit enables to assess the post migration Transactional store and Analytical store Cosmos DB containers making use of the Cosmos DB Spark connectors. 

# Features
1. Compare the record count across Source Transactional store, Sink Transactional store and Sink Analytical store using PartitionStatistics 

2. Schema comparison across Transactional and Analytical Stores

3. cell-by-cell comparison for the configured number of records 

4. Accepts the custom queries from end-user and compare results by executing them against Source Transactional store, Sink Transactional store and Sink Analytical store 

5. Persists the assessment comparison results to a Spark table

6. Execute against Databricks and Synapse runtimes.

7. Config Driven: The features and the Application arguments are driven by configs. 

8. Automated Remote Execution on Databricks and Synapse clusters from Local Box

9. Easily extendable and customizable. 


# Getting Started

1. Get the latest jar from [here](https://revin.blob.core.windows.net/cosmosdb-post-migration-assessment-toolkit/cosmosdb-post-migration-assessment-toolkit-1.0-SNAPSHOT-uber.jar)
2. Install the jar under Libraries on Databricks cluster and get the dbfs path. 
3. Create the Config file with the required options. Below is a sample config file used for executions on Databricks.

```
{
  "databricksUri": "https://eastus.azuredatabricks.net",
  "databricksToken": "xxxxx",
  "existingClusterId": "xxxxx",
  "jarFilePath": "dbfs:/FileStore/jars/xxxxx-cosmosdb_post_migration_assessment_toolkit_1_0_SNAPSHOT_uber-77eab.jar",
  "applicationArgs": [
	"--srcCosmosEndpoint",
	"https://xxxxx.documents.azure.com:443/",
	"--srcCosmosAccountKey",
	"xxxxx",
	"--srcCosmosRegion",
	"WestUS2",
	"--srcCosmosDatabaseName",
	"xxxxx",
	"--srcCosmosCollectionName",
	"xxxxx",	
	"--sinkCosmosEndpoint",
	"https://xxxxx.documents.azure.com:443/",
	"--sinkCosmosAccountKey",
	"xxxxx",
	"--sinkCosmosRegion",
	"WestUS2",
	"--sinkCosmosDatabaseName",
	"xxxxx",
	"--sinkCosmosCollectionName",
	"xxxxx",	
	"--resultsDBName",
	"resultsDB",
	"--resultsTableName",
	"resultsTable",
	"--skipCustomQueryExecution",
	"true"]
}
```

```
  a. databricksUri: Azure Databricks workspace endpoint.
  b. databricksToken: Personal access tokens for secure authentication to the Databricks API. You can generate this from User Settings in Azure Databricks workspace.
  c. existingClusterId: Get the cluster id from Tags tab.
  d. The jar file path uploaded to the dbfs.
  e. Arguments to the spark jobs submitted to the cluster. These are provided in applicationArgs field.
  f. custom spark-sql queries can be stored in a file and executed against the collections. Multiple queries can be separated by "#".
     Eg: SELECT max(Latitude) as max_Latitude from c # SELECT min(Longitude) as min_Longitude from c
```   


### Sample Config file for Custom assessment Query Execution:

``` 
{
  "databricksUri": "https://eastus.azuredatabricks.net",
  "databricksToken": "xxxxx",
  "existingClusterId": "xxxxx",
  "jarFilePath": "dbfs:/FileStore/jars/xxxxx-cosmosdb_post_migration_assessment_toolkit_1_0_SNAPSHOT_uber-77eab.jar",
  "applicationArgs": [
	"--srcCosmosEndpoint",
	"https://xxxxx.documents.azure.com:443/",
	"--srcCosmosAccountKey",
	"xxxxx",
	"--srcCosmosRegion",
	"WestUS2",
	"--srcCosmosDatabaseName",
	"xxxxx",
	"--srcCosmosCollectionName",
	"xxxxx",	
	"--sinkCosmosEndpoint",
	"https://xxxxx.documents.azure.com:443/",
	"--sinkCosmosAccountKey",
	"xxxxx",
	"--sinkCosmosRegion",
	"WestUS2",
	"--sinkCosmosDatabaseName",
	"xxxxx",
	"--sinkCosmosCollectionName",
	"xxxxx",	
	"--resultsDBName",
	"resultsDB",
	"--resultsTableName",
	"resultsTable",
	"--skipCustomQueryExecution",
	"false",
	"--customQueryPath",
	"C:\\cosmosdb\\cosmosdb-post-migration-assessment-toolkit\\queries.sql",
	"--srcCutoffIdentifierField",
	"id",			
	"--cellComparisonWhereClause",
	"deviceid = 'dev1'",
	"--cellComparisonRecordsLimit",
	"10",
	"--cellComparisonSortField",
	"id"]
}
``` 

6. Below is a sample execution from a terminal, which starts a java process and submits spark job to the cluster

``` 
java -cp local\path\to\cosmosdb-post-migration-assessment-toolkit-1.0-SNAPSHOT-uber.jar cosmosdb.Runner local\path\to\config.json databricks
```



 
