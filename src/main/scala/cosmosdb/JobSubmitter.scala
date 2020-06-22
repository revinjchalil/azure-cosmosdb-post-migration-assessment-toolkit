package cosmosdb

import com.typesafe.scalalogging.LazyLogging
import cosmosdb.JobStatus.JobStatus
/**
 * An interface for running  jobs.
 */
trait JobSubmitter extends LazyLogging {

  /**
   * Run a Job.
   * @param job Job to be run
   * @return Return status of Job after completion
   */
  def runJob(job: Job): JobStatus

//  /**
//   * Validation of flags and their values.
//   * @param applicationArgs Arguments to spark application
//   * @param conf spark conf
//   */
//  def localValidation(applicationArgs: ToolkitGeneratorOptions,
//                      conf: Map[String, String]): Unit

  /**
   * Close running threads
   */
  def close(): Unit

}

/**
 * Case class for Job
 * @param name Job Name
 * @param className class name of the main class
 * @param jarFilePath path for the jar file
 * @param applicationArgs params required for job
 * @param conf spark conf
 */
case class Job(name: String,
               className: String,
               jarFilePath: String,
               applicationArgs: Array[String],
               conf: Map[String, String])

/**
 * An enum for job status
 */
object JobStatus extends Enumeration {
  type JobStatus = Value
  val SUCCEEDED = Value("SUCCEEDED")
  val FAILED = Value("FAILED")
  val RUNNING = Value("RUNNING")
}

/**
 * An enum for job submitter type
 */
object JobSubmitter extends Enumeration {
  type JobSubmitterType = Value

//  val LIVY = Value("LIVY")
  val SYNAPSE = Value("SYNAPSE")
  val DATABRICKS = Value("DATABRICKS")
  val UNDEFINED = Value("UNDEFINED")

  def getJobSubmitterType(s: String): JobSubmitter.JobSubmitterType = {
    if (values.exists(_.toString == s)) {
      JobSubmitter withName s
    } else {
      JobSubmitter.UNDEFINED
    }
  }
}
