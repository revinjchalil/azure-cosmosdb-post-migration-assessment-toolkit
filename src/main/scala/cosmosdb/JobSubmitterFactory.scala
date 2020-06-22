package cosmosdb


class JobSubmitterFactory() {

  def get(parsedOptions: ParsedOptions): JobSubmitter = {
    parsedOptions match {
//      case parsedOptions: HdiParsedOptions => HdiJobSubmitter(parsedOptions)
//      case parsedOptions: SynapseParsedOptions => SynapseJobSubmitter(parsedOptions)
      case parsedOptions: DatabricksParsedOptions => DatabricksJobSubmitter(parsedOptions)
      case _ => throw new RuntimeException("Invalid Job Submitter.")
    }
  }
}

object JobSubmitterFactory {
  def apply(): JobSubmitterFactory = new JobSubmitterFactory()
}