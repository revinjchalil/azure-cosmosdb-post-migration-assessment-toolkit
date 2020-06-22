package cosmosdb

class OptionsParserFactory() {

  def get(jobType: JobSubmitter.JobSubmitterType): OptionsParser = {
    jobType match {
//      case JobSubmitter.SYNAPSE => SynapseOptionsParser()
      case JobSubmitter.DATABRICKS => DatabricksOptionsParser()
      case _ => throw new RuntimeException("Invalid Job Submitter.")
    }
  }
}

object OptionsParserFactory {
  def apply(): OptionsParserFactory = new OptionsParserFactory()
}
