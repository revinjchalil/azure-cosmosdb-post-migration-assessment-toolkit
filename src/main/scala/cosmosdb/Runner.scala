package cosmosdb

import com.typesafe.scalalogging.LazyLogging
import cosmosdb.JobStatus.JobStatus
import scala.io.Source

class Runner(jobSubmitter: JobSubmitter,
             jarFilePath: String,
             applicationArgs: ToolkitGeneratorOptions,
             conf: Map[String, String],
             toolkitOptionsParser: ToolkitOptionsParser) extends LazyLogging {

  def run(): Unit = {
    invokeQueryExecution()
  }

  private def invokeQueryExecution(): Unit = {
    logger.info("Invoking Query Execution")
    var applicationArgsLocal: ToolkitGeneratorOptions = null
    if (!applicationArgs.skipCustomQueryExecution) {
      val customQueryPath = applicationArgs.customQueryPath.get.trim
      applicationArgsLocal = applicationArgs.copy(
        queriesToRun = getQueryString(customQueryPath))
    }
    else {
      applicationArgsLocal = applicationArgs.copy(
        queriesToRun = "")
    }

    val job = Job("Assessment Execution Job",
      "cosmosdb.AssessmentExecutor",
      jarFilePath,
      toolkitOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("\t"),
      conf)
    val jobStatus = jobSubmitter.runJob(job)
          validateSuccess(jobStatus)
    //      logger.info(s"Job for $queryName finished with $jobStatus")
    //    }
  }

  private def validateSuccess(jobStatus: JobStatus) {
    logger.info(s"Job finished with status $jobStatus.")
    if (jobStatus != JobStatus.SUCCEEDED) {
      throw new RuntimeException(s"Job failed with status $jobStatus." +
        s" Can't proceed further, please check the application logs for error details")
    }
  }

  private def getQueryString(pathToResources: String): String = {
    try {
      val source = Source.fromFile(pathToResources)
      val queryString: String = source.getLines.mkString(" ")
      source.close()
      queryString
    } catch {
      case exception: Exception => throw new RuntimeException(s"Query Path doesn't exists " +
        s"$pathToResources. Message ${exception.getMessage}")
    }
  }
}

object Runner {
  def apply(parsedOptions: ParsedOptions, jobSubmitter: JobSubmitter): Runner = {
    val perfSuiteOptionsParser: ToolkitOptionsParser = ToolkitOptionsParser()
    val applicationArgs = parsedOptions.applicationArgs
    new Runner(jobSubmitter, parsedOptions.jarFilePath, applicationArgs,
      parsedOptions.conf, perfSuiteOptionsParser)
  }

  def main(args: Array[String]): Unit = {
    var jobSubmitter: JobSubmitter = null
    try {
      val confFilePath = args(0)
      val jobSubmitterType =
        if (args.length == 2) JobSubmitter.getJobSubmitterType(args(1).toUpperCase())
        else JobSubmitter.DATABRICKS

      val parsedOptions = OptionsParserFactory().get(jobSubmitterType).parse(confFilePath)
      jobSubmitter = JobSubmitterFactory().get(parsedOptions)
      Runner(parsedOptions, jobSubmitter).run()
    } catch {
      case e: Exception =>
        println(s"Failed with error: ${e.printStackTrace()}")
    } finally {
      jobSubmitter.close()
    }
  }
}
