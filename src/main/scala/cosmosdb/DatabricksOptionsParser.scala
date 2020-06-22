package cosmosdb

/**
 * Databricks Options Parser.
 *
 * @param toolkitOptionsParser Perf Suite option parser
 */
class DatabricksOptionsParser(toolkitOptionsParser: ToolkitOptionsParser)
  extends OptionsParser {

  override def parse(pathToOptionsJson: String): DatabricksParsedOptions = {
    var runnerOptions = parseJson[DatabricksRunnerOptions](pathToOptionsJson)

//    if (runnerOptions.jarFilePath == null) {
//      runnerOptions =
//        runnerOptions.copy(jarFilePath = config.defaultJarFilePath)
//    }
    DatabricksParsedOptions(
      runnerOptions.jarFilePath,
      toolkitOptionsParser.parseOptions(runnerOptions.applicationArgs),
      runnerOptions.conf,
      runnerOptions.databricksUri,
      runnerOptions.newCluster,
      runnerOptions.existingClusterId,
      runnerOptions.databricksToken
    )
  }
}

object DatabricksOptionsParser {
  def apply(): DatabricksOptionsParser =
    new DatabricksOptionsParser(ToolkitOptionsParser())
}

case class DatabricksParsedOptions(jarFilePath: String,
                                   applicationArgs: ToolkitGeneratorOptions,
                                   conf: Map[String, String],
                                   databricksUri: String,
                                   newCluster: Map[String, Object],
                                   existingClusterId : String,
                                   databricksToken: String
                                     ) extends ParsedOptions

case class DatabricksRunnerOptions(jarFilePath: String,
                                           applicationArgs: Array[String],
                                           conf: Map[String, String],
                                           databricksUri: String,
                                           newCluster: Map[String, Object],
                                           existingClusterId: String,
                                           databricksToken: String
                                          ) extends RunnerOptions