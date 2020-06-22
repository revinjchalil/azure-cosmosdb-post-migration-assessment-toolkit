package cosmosdb

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.cli.{BasicParser, Options}
import scala.util.Try

/**
 * Toolkit Options Parser class to define and parse cli arguments.
 * @param options options.
 * @param parser  options parser.
 */
class ToolkitOptionsParser(options: Options, parser: BasicParser) {
  val srcCosmosEndpointParamName = "srcCosmosEndpoint"
  val srcCosmosAccountKeyParamName = "srcCosmosAccountKey"
  val srcCosmosRegionParamName = "srcCosmosRegion"
  val srcCosmosDatabaseNameParamName = "srcCosmosDatabaseName"
  val srcCosmosCollectionNameParamName = "srcCosmosCollectionName"
  val sinkCosmosEndpointParamName = "sinkCosmosEndpoint"
  val sinkCosmosAccountKeyParamName = "sinkCosmosAccountKey"
  val sinkCosmosRegionParamName = "sinkCosmosRegion"
  val sinkCosmosDatabaseNameParamName = "sinkCosmosDatabaseName"
  val sinkCosmosCollectionNameParamName = "sinkCosmosCollectionName"
  val resultsDBNameParamName = "resultsDBName"
  val resultsTableNameParamName = "resultsTableName"
  val skipCustomQueryExecutionParamName = "skipCustomQueryExecution"
  val customQueryPathParamName = "customQueryPath"
  val queriesToRunParamName = "queriesToRun"
  val srcCutoffIdentifierFieldParamName = "srcCutoffIdentifierField"
  val cellComparisonWhereClauseParamName = "cellComparisonWhereClause"
  val cellComparisonRecordsLimitParamName = "cellComparisonRecordsLimit"
  val cellComparisonSortFieldParamName = "cellComparisonSortField"

  options.addOption(
    "sep",
    srcCosmosEndpointParamName,
    true,
    "srcCosmosEndpointParamName"
  ).addOption(
    "sak",
    srcCosmosAccountKeyParamName,
    true,
    "srcCosmosAccountKeyParamName"
  ).addOption(
    "sr",
    srcCosmosRegionParamName,
    true,
    "srcCosmosRegionParamName"
  ).
    addOption(
      "sdb",
      srcCosmosDatabaseNameParamName,
      true,
      "srcCosmosDatabaseNameParamName"
    ).addOption(
    "sc",
    srcCosmosCollectionNameParamName,
    true,
    "srcCosmosCollectionNameParamName"
  ).addOption(
    "dep",
    sinkCosmosEndpointParamName,
    true,
    "sinkCosmosEndpointParamName"
  ).addOption(
    "dak",
    sinkCosmosAccountKeyParamName,
    true,
    "sinkCosmosAccountKeyParamName"
  ).addOption(
    "dr",
    sinkCosmosRegionParamName,
    true,
    "sinkCosmosRegionParamName"
  ).addOption(
    "ddb",
    sinkCosmosDatabaseNameParamName,
    true,
    "sinkCosmosDatabaseNameParamName"
  ).addOption(
    "dc",
    sinkCosmosCollectionNameParamName,
    true,
    "sinkCosmosCollectionNameParamName"
  ).addOption(
    "dc",
    skipCustomQueryExecutionParamName,
    true,
    "skipCustomQueryExecutionParamName"
  ).addOption(
    "cqp",
    customQueryPathParamName,
    true,
    "customQueryPathParamName"
  ).addOption(
    "cqp",
    queriesToRunParamName,
    true,
    "queriesToRunParamName"
  ).addOption(
    "cif",
     srcCutoffIdentifierFieldParamName,
    true,
    "srcCutoffIdentifierFieldParamName"
  ).addOption(
    "rdb",
    resultsDBNameParamName,
    true,
    "resultsDBNameParamName"
  ).addOption(
    "rtb",
    resultsTableNameParamName,
    true,
    "resultsTableNameParamName"
  ).addOption(
    "ccw",
    cellComparisonWhereClauseParamName,
    true,
    "cellComparisonWhereClauseParamName"
  ).addOption(
    "ccl",
    cellComparisonRecordsLimitParamName,
    true,
    "cellComparisonRecordsLimitParamName"
  ).addOption(
    "ccs",
    cellComparisonSortFieldParamName,
    true,
    "cellComparisonSortFieldParamName"
  )

  /**
   * Parses the cli arguments.
   * @param args cli arguments
   * @return parsed ToolkitGeneratorOptions.
   */
  def parseOptions(args: Array[String]): ToolkitGeneratorOptions = {
    val cmd = parser.parse(options, args)

    ToolkitGeneratorOptions(cmd.getOptionValue(srcCosmosEndpointParamName),
      cmd.getOptionValue(srcCosmosAccountKeyParamName),
      cmd.getOptionValue(srcCosmosRegionParamName),
      cmd.getOptionValue(srcCosmosDatabaseNameParamName),
      cmd.getOptionValue(srcCosmosCollectionNameParamName),
      cmd.getOptionValue(sinkCosmosEndpointParamName),
      cmd.getOptionValue(sinkCosmosAccountKeyParamName),
      cmd.getOptionValue(sinkCosmosRegionParamName),
      cmd.getOptionValue(sinkCosmosDatabaseNameParamName),
      cmd.getOptionValue(sinkCosmosCollectionNameParamName),
      cmd.getOptionValue(resultsDBNameParamName),
      cmd.getOptionValue(resultsTableNameParamName),
      cmd.getOptionValue(skipCustomQueryExecutionParamName, config.defaultForskipCustomQueryExecution).toBoolean,
      Try(cmd.getOptionValue(customQueryPathParamName)).toOption,
      cmd.getOptionValue(queriesToRunParamName, ""),
      Try(cmd.getOptionValue(srcCutoffIdentifierFieldParamName)).toOption,
      Try(cmd.getOptionValue(cellComparisonWhereClauseParamName)).toOption,
      Try(cmd.getOptionValue(cellComparisonRecordsLimitParamName)).toOption,
      Try(cmd.getOptionValue(cellComparisonSortFieldParamName)).toOption
    )
  }

  def deserializeParsedOptions(parsedOptions: ToolkitGeneratorOptions): String = {
    var returnValue = s"--$srcCosmosEndpointParamName\t${parsedOptions.srcCosmosEndpoint}\t" +
      s"--$srcCosmosAccountKeyParamName\t${parsedOptions.srcCosmosAccountKey}\t" +
      s"--$srcCosmosRegionParamName\t${parsedOptions.srcCosmosRegion}\t" +
      s"--$srcCosmosDatabaseNameParamName\t${parsedOptions.srcCosmosDatabaseName}\t" +
      s"--$srcCosmosCollectionNameParamName\t${parsedOptions.srcCosmosCollectionName}\t" +
      s"--$sinkCosmosEndpointParamName\t${parsedOptions.sinkCosmosEndpoint}\t" +
      s"--$sinkCosmosAccountKeyParamName\t${parsedOptions.sinkCosmosAccountKey}\t" +
      s"--$sinkCosmosRegionParamName\t${parsedOptions.sinkCosmosRegion}\t" +
      s"--$sinkCosmosDatabaseNameParamName\t${parsedOptions.sinkCosmosDatabaseName}\t" +
      s"--$sinkCosmosCollectionNameParamName\t${parsedOptions.sinkCosmosCollectionName}\t" +
      s"--$resultsDBNameParamName\t${parsedOptions.resultsDBName}\t" +
      s"--$resultsTableNameParamName\t${parsedOptions.resultsTableName}\t" +
      s"--$skipCustomQueryExecutionParamName\t${parsedOptions.skipCustomQueryExecution}\t" +
      s"--$customQueryPathParamName\t${parsedOptions.customQueryPath}\t" +
      s"--$queriesToRunParamName\t${parsedOptions.queriesToRun}\t" +
      s"--$srcCutoffIdentifierFieldParamName\t${parsedOptions.srcCutoffIdentifierField}\t" +
      s"--$cellComparisonWhereClauseParamName\t${parsedOptions.cellComparisonWhereClause}\t" +
      s"--$cellComparisonRecordsLimitParamName\t${parsedOptions.cellComparisonRecordsLimit}\t" +
      s"--$cellComparisonSortFieldParamName\t${parsedOptions.cellComparisonSortField}\t"

    returnValue
  }

  private def getCurrentTimestamp(): String = {
    new SimpleDateFormat("YYYYMMdd_HHmmss").format(new Date())
  }
}

/**
 * Companion object.
 */
object ToolkitOptionsParser {
  val options = new Options
  val parser = new BasicParser
  def apply(): ToolkitOptionsParser = new ToolkitOptionsParser(options, parser)
}

case class ToolkitGeneratorOptions(srcCosmosEndpoint: String,
                                   srcCosmosAccountKey: String,
                                   srcCosmosRegion: String,
                                   srcCosmosDatabaseName: String,
                                   srcCosmosCollectionName: String,
                                   sinkCosmosEndpoint: String,
                                   sinkCosmosAccountKey: String,
                                   sinkCosmosRegion: String,
                                   sinkCosmosDatabaseName: String,
                                   sinkCosmosCollectionName: String,
                                   resultsDBName: String,
                                   resultsTableName: String,
                                   skipCustomQueryExecution: Boolean,
                                   customQueryPath: Option[String],
                                   queriesToRun: String,
                                   srcCutoffIdentifierField: Option[String],
                                   cellComparisonWhereClause: Option[String],
                                   cellComparisonRecordsLimit: Option[String],
                                   cellComparisonSortField: Option[String])
