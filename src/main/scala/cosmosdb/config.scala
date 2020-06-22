package cosmosdb

/**
 * Configurations used throughout the program.
 */
package object config {

  /**
   * Separator used for custom assessment queries .
   */
  private[cosmosdb] val queryNameSeparator = "#"

  /**
   * Interval to check for active jobs to finish, waiting to submit the job.
   */
  private[cosmosdb] val livyActiveJobPollingIntervalInSeconds = 30

  /**
   * Default value for Skip Custom QueryE xecution Config.
   */
  private[cosmosdb] val defaultForskipCustomQueryExecution = "true"

  /**
   * Number of retry to livy api
   */
  private[cosmosdb] val livyRetryCount = 4

  /**
   * cosmosOLTP
   */
  private[cosmosdb] val cosmosOLTP = "cosmos.oltp"


  /**
   * cosmosOLAP
   */
  private[cosmosdb] val cosmosOLAP = "cosmos.olap"

}