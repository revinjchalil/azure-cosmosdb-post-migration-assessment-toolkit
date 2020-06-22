package cosmosdb

import java.net.HttpURLConnection
import java.util.concurrent.TimeUnit

import cosmosdb.JobStatus.JobStatus

/**
 * This class implements job submitter for Azure Databricks
 * @param databricksUri databricks workspace URL
 * @param newCluster A map of new cluster details (provided in case of using automated clusters)
 * @param existingClusterId Existing cluster id (provided in case of using interactive cluster,
 *                          preferred over newCluster if both are provided)
 * @param databricksToken Personal Access Token for databricks workspace
 * @param databricksRequestBodyFactory databricks request body factory
 * @param httpUtil a utility for creating http requests
 */
class DatabricksJobSubmitter(databricksUri: String,
                             newCluster: Map[String, Object],
                             existingClusterId: String,
                             databricksToken: String,
                             databricksRequestBodyFactory: DatabricksRequestBodyFactory,
                             httpUtil: HttpUtil) extends JobSubmitter {

  var jobStatus: JobStatus = _

  /**
   * Run a Job.
   * @param job Job to be run
   * @return Return status of Job after completion
   */
  override def runJob(job: Job): JobStatus = {
    val requestBody = databricksRequestBodyFactory.getDatabricksRequest(job, newCluster,
      existingClusterId)
    logger.info(s"Request Body: $requestBody")
    val runId: Int = submitJob(getAuthHeader(), requestBody,
      HttpURLConnection.HTTP_OK, config.livyRetryCount)
    pollJobCompletion(runId)
    jobStatus
  }

  private def submitJob(authHeader: String, requestBody: String, expectedResponseCode: Int,
                        retryCount: Int): Int = {
    val endpoint: String = databricksUri + DatabricksJobSubmitter.createRunEndpoint
    logger.info(s"Create endpoint: $endpoint")
    val httpURLConnection: HttpURLConnection = httpUtil.createPostConnection(endpoint, authHeader,
      requestBody, expectedResponseCode, retryCount)
    val response = httpUtil.readResponse(httpURLConnection)
    httpUtil.getJsonField(response, "/run_id").toInt
  }

  private def pollJobCompletion(runId: Int): Unit = {
    while (checkInProgressJob(runId, getAuthHeader())) {
      TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
    }
  }

  /**
   * Make a databricks api call to get the job status.
   *
   * @return true if there are 'running' or 'starting' batch sessions, else false.
   */
  private def checkInProgressJob(runId: Int,
                                 authHeader: String): Boolean = {
    val endpoint: String = databricksUri + DatabricksJobSubmitter.getRunDetailsEndpoint.format(
      runId)
    logger.info(s"Get endpoint: $endpoint")
    val httpURLConnection = httpUtil.createGetConnection(endpoint, authHeader,
      config.livyRetryCount)
    val response = httpUtil.readResponse(httpURLConnection)
    val lifeCycleState = httpUtil.getJsonField(response, "/state/life_cycle_state")
    val stateMessage = httpUtil.getJsonField(response, "/state/state_message")
    logger.info(s"Life Cycle State: $lifeCycleState, message: $stateMessage")
    jobStatus = getJobStatus(lifeCycleState)
    if (jobStatus == JobStatus.RUNNING) {
      true
    } else {
      val resultState = httpUtil.getJsonField(response, "/state/result_state")
      logger.info(s"Result State: $resultState")
      if (!resultState.isEmpty) {
        jobStatus = resultState.toUpperCase() match {
          case "SUCCESS" => JobStatus.SUCCEEDED
          case _ => JobStatus.FAILED
        }
      }
      if (jobStatus == JobStatus.FAILED) {
        logger.error(s"Job failed. Response is $response")
      }
      false
    }
  }

  /**
   * Map result state to job status enum
   *
   * @param status result state
   * @return Job status
   */
  private def getJobStatus(status: String): JobStatus = {
    status.toUpperCase() match {
      case "TERMINATED" => JobStatus.SUCCEEDED
      case "INTERNAL_ERROR" | "SKIPPED" => JobStatus.FAILED
      case _ => JobStatus.RUNNING
    }
  }

//  override def localValidation(applicationArgs: ToolkitGeneratorOptions,
//                               conf: Map[String, String]): Unit = {
//    new DatabricksLocalValidation(applicationArgs.skipDataGeneration,
//      applicationArgs.skipMetastoreCreation,
//      applicationArgs.skipQueryExecution,
//      applicationArgs.dataGenPartitionsToRun,
//      applicationArgs.systemMetricsPort,
//      applicationArgs.enableSystemMetrics,
//      applicationArgs.enableProfiling,
//      newCluster,
//      existingClusterId).validate()
//  }


  /**
   * @return auth token.
   */
  private def getAuthHeader(): String = {
    val authHeaderValue: String = "Bearer " + new String(databricksToken)
    authHeaderValue
  }

  /**
   * Close running threads
   */
  override def close(): Unit = {}
}

object DatabricksJobSubmitter {
  val createRunEndpoint: String = "/api/2.0/jobs/runs/submit"
  val getRunDetailsEndpoint: String = "/api/2.0/jobs/runs/get?run_id=%s"

  def apply(parsedOptions: DatabricksParsedOptions): DatabricksJobSubmitter = {
    new DatabricksJobSubmitter(parsedOptions.databricksUri, parsedOptions.newCluster,
      parsedOptions.existingClusterId, parsedOptions.databricksToken,
      DatabricksRequestBodyFactory(), HttpUtil())
  }
}
