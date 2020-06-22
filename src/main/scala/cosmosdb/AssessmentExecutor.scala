package cosmosdb

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import com.microsoft.azure.documentdb.{DocumentClient,  RequestOptions}
import org.joda.time.DateTime
import scala.collection.JavaConversions._

class AssessmentExecutor(sparkSession: SparkSession,
                         srcCosmosEndpoint: String,
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
                         queriesToRun: String,
                         srcCutoffIdentifierField: Option[String],
                         cellComparisonWhereClause: Option[String],
                         cellComparisonRecordsLimit: Option[String],
                         cellComparisonSortField: Option[String],
                         dfsrcTS: DataFrame,
                         dfSinkTS: DataFrame,
                         dfSinkAS: DataFrame) extends Logging {

  import sparkSession.implicits._

  private def execute()  = {
    try {
      createResultsTableIfNotExists()
      compareRowCountsInternal()
      compareColumnCountsInternal()
      compareSchema()
      compareLimitedCells()

      if(!skipCustomQueryExecution) {
        val queries = queriesToRun.split(config.queryNameSeparator)
        queries.foreach { queryString =>
          executeSingleQuery(queryString.trim)
        }
      }
    } catch {
      case exception: Exception => throw new RuntimeException(s"Execution failed. StackTrace: ${exception.printStackTrace()}")
    }
  }

  private def execDuration[R](code: => R, t: Long = System.currentTimeMillis()): (R, Long) = {
    val result = code    // call-by-name
    (result, (System.currentTimeMillis() - t))
  }

  private def compareRowCountsInternal() : Unit = {
    try {
      val startTimeLocal = DateTime.now().toString()
      val srcClient = new DocumentClient(srcCosmosEndpoint, srcCosmosAccountKey, null, null)
      val sinkClient = new DocumentClient(sinkCosmosEndpoint, sinkCosmosAccountKey, null, null)
      val srcCollectionLink = s"/dbs/${srcCosmosDatabaseName}/colls/${srcCosmosCollectionName}"
      val sinkCollectionLink = s"/dbs/${sinkCosmosDatabaseName}/colls/${sinkCosmosCollectionName}"
      val options = new RequestOptions
      options.setPopulatePartitionKeyRangeStatistics(true)
      val srcCollection = srcClient.readCollection(srcCollectionLink, options).getResource
      val sinkCollection = sinkClient.readCollection(sinkCollectionLink, options).getResource

      val (srcTSCollectionCount, srcTSExecDuration) = execDuration { srcCollection.getCollectionPartitionStatistics.map(_.getDocumentCount).sum }
      val (sinkTSCollectionCount, sinkTSExecDuration) = execDuration { sinkCollection.getCollectionPartitionStatistics.map(_.getDocumentCount).sum }
      val (sinkASCollectionCount, sinkASExecDuration) = execDuration {
        sparkSession.sql("select count(*) from dfSinkAS").collect().map(_ (0)).toList.head.asInstanceOf[Long]
      }

      val overallResult = if (srcTSCollectionCount == sinkTSCollectionCount && sinkTSCollectionCount == sinkASCollectionCount) true else false

      val endTimeLocal = DateTime.now().toString()

      val queryResults = Seq(QueryResultsNumbers("row-count", "internal", startTimeLocal, endTimeLocal, srcTSCollectionCount
        , srcTSExecDuration, sinkTSCollectionCount, sinkTSExecDuration, sinkASCollectionCount, sinkASExecDuration, overallResult)).toDF()
      queryResults.createOrReplaceTempView("queryResults")
      val metricsInsertScript = s"insert into ${resultsDBName}.${resultsTableName} select * from queryResults"
      sparkSession.sql(metricsInsertScript)
    } catch {
      case exception: Exception => throw new RuntimeException(s"Execution failed for compareRowCountsInternal. ${exception.printStackTrace()}")
    }
  }

  private def compareColumnCountsInternal() : Unit = {
    try {
      val startTimeLocal = DateTime.now().toString()

      val (srcTSColumnCount, srcTSExecDuration) = execDuration { dfsrcTS.columns.size }
      val (sinkTSColumnCount, sinkTSExecDuration) = execDuration { dfSinkTS.columns.size }
      val (sinkASColumnCount, sinkASExecDuration) = execDuration { dfSinkAS.columns.size }

      val overallResult = if (srcTSColumnCount == sinkTSColumnCount && sinkTSColumnCount == sinkASColumnCount) true else false

      val endTimeLocal = DateTime.now().toString()

      val queryResults = Seq(QueryResultsNumbers("column-count", "internal", startTimeLocal, endTimeLocal, srcTSColumnCount
        , srcTSExecDuration, sinkTSColumnCount, sinkTSExecDuration, sinkASColumnCount, sinkASExecDuration, overallResult)).toDF()
      queryResults.createOrReplaceTempView("queryResults")
      val metricsInsertScript = s"insert into ${resultsDBName}.${resultsTableName} select * from queryResults"
      sparkSession.sql(metricsInsertScript)
    } catch {
      case exception: Exception => throw new RuntimeException(s"Execution failed for compareColumnCountsInternal. ${exception.printStackTrace()}")
    }
  }

  private def compareSchema() : Unit = {
    try {
      val startTimeLocal = DateTime.now().toString()

      val (srcTSSchema, srcTSExecDuration) = execDuration {
        dfsrcTS.drop("_ts").schema.fields.map(f => (s"${f.name}-${f.dataType}-${f.nullable}"))
      }
      val (sinkTSSchema, sinkTSExecDuration) = execDuration {
        dfSinkTS.drop("_ts").schema.fields.map(f => (s"${f.name}-${f.dataType}-${f.nullable}"))
      }
      val (sinkASSchema, sinkASExecDuration) = execDuration {
        dfSinkAS.drop("_ts").schema.fields.map(f => (s"${f.name}-${f.dataType}-${f.nullable}"))
      }

      scala.util.Sorting.quickSort(srcTSSchema)
      scala.util.Sorting.quickSort(sinkTSSchema)
      scala.util.Sorting.quickSort(sinkASSchema)

      val overallResult = if (srcTSSchema.diff(sinkTSSchema).isEmpty && srcTSSchema.length == sinkTSSchema.length &&
        sinkTSSchema.diff(sinkASSchema).isEmpty && sinkTSSchema.length == sinkASSchema.length) true else false

      val endTimeLocal = DateTime.now().toString()

      val queryResults = Seq(QueryResultsArray("schema-compare", "internal", startTimeLocal, endTimeLocal, srcTSSchema
        , srcTSExecDuration, sinkTSSchema, sinkTSExecDuration, sinkASSchema, sinkASExecDuration, overallResult))
        .toDF()
      queryResults.createOrReplaceTempView("queryResults")
      val metricsInsertScript = s"insert into ${resultsDBName}.${resultsTableName} select * from queryResults"
      sparkSession.sql(metricsInsertScript)
    } catch {
      case exception: Exception => throw new RuntimeException(s"Execution failed for compareSchema. ${exception.printStackTrace()}")
    }
  }

  private def compareLimitedCells() : Unit = {
    try {
      val startTimeLocal = DateTime.now().toString()

      val cellCompareQueryString = "select * from dfsrcTS order by 1 limit 1"
//      if (cellComparisonWhereClause.isDefined) cellCompareQueryString += s" where ${cellComparisonWhereClause.get.toString}"
//      if (cellComparisonSortField.isDefined) cellCompareQueryString += s" order by ${cellComparisonSortField.get.toString}" else cellCompareQueryString += s" order by 1"
//      if (cellComparisonRecordsLimit.isDefined) cellCompareQueryString += s" limit ${cellComparisonRecordsLimit.get.toString}" else cellCompareQueryString += s" limit 10"

      var dfSrcTSCellsResults = sparkSession.emptyDataFrame
      val (srcTSCellsResults, srcTSExecDuration) = execDuration {
        val dfSrcTSCells = sparkSession.sql(cellCompareQueryString).drop("_ts")
        val sortedCols = dfSrcTSCells.columns.sorted.map(str => dfSrcTSCells.col(str))
//        val cols = dfSrcTSCells.columns.map(dfSrcTSCells(_)).reverse
        println(s"SrcTScols: ${sortedCols}")
        dfSrcTSCellsResults = dfSrcTSCells.select(sortedCols:_*)
        dfSrcTSCellsResults.toJSON.collect()
      }

      var dfSinkTSCellsResults = sparkSession.emptyDataFrame
      val (sinkTSCellsResults, sinkTSExecDuration) = execDuration {
        val dfSinkTSCells = sparkSession.sql(cellCompareQueryString.replace("dfsrcTS", "dfsinkTS")).drop("_ts")
        val sortedCols = dfSinkTSCells.columns.sorted.map(str => dfSinkTSCells.col(str))
        println(s"SinkTScols: ${sortedCols}")
        dfSinkTSCellsResults = dfSinkTSCells.select(sortedCols:_*)
        dfSinkTSCellsResults.toJSON.collect()
      }

      var dfSinkASCellsResults = sparkSession.emptyDataFrame
      val (sinkASCellsResults, sinkASExecDuration) = execDuration {
        val dfSinkASCells = sparkSession.sql(cellCompareQueryString.replace("dfsrcTS", "dfsinkAS")).drop("_ts")
        val sortedCols = dfSinkASCells.columns.sorted.map(str => dfSinkASCells.col(str))
        println(s"SinkAScols: ${sortedCols}")
        dfSinkASCellsResults = dfSinkASCells.select(sortedCols:_*)
        dfSinkASCellsResults.toJSON.collect()
      }

      val overallResult = if ((dfSrcTSCellsResults.exceptAll(dfSinkTSCellsResults).count() == 0 &&
        dfSinkTSCellsResults.exceptAll(dfSrcTSCellsResults).count() == 0 &&
        dfSinkTSCellsResults.exceptAll(dfSinkASCellsResults).count() == 0 &&
        dfSinkASCellsResults.exceptAll(dfSinkTSCellsResults).count() == 0)) true else false

      val endTimeLocal = DateTime.now().toString()

      val queryResults = Seq(QueryResultsArray("cell-compare", "internal", startTimeLocal, endTimeLocal, srcTSCellsResults
        , srcTSExecDuration, sinkTSCellsResults, sinkTSExecDuration, sinkASCellsResults, sinkASExecDuration, overallResult))
        .toDF()
      queryResults.createOrReplaceTempView("queryResults")
      val metricsInsertScript = s"insert into ${resultsDBName}.${resultsTableName} select * from queryResults"
      sparkSession.sql(metricsInsertScript)
    } catch {
      case exception: Exception => throw new RuntimeException(s"Execution failed for compareLimitedCells. ${exception.printStackTrace()}")
    } finally {
      sparkSession.catalog.clearCache()
    }
    }


  private def executeSingleQuery(queryString: String) = {
      logInfo(s"The query from file is $queryString")

      try {
        val startTimeLocal = DateTime.now().toString()

        var dfSrcTSResults = sparkSession.emptyDataFrame
        val (srcTSResults, srcTSExecDuration) = execDuration {

          val srcTSQueryString = (queryString.replace("from c", "from dfsrcTS")
            .replace("FROM c", "from dfsrcTS").replace("from c", "from dfsrcTS"))
          var srcTSQueryStringCutOff = srcTSQueryString

          if (srcCutoffIdentifierField.isDefined) {
            val sinkLatestId = sparkSession.sql(s"select ${srcCutoffIdentifierField.get.toString} from dfSinkAS where _ts in " +
              s"(select max(_ts) from dfSinkAS)").first().get(0)
            logInfo(s"sinkLatestId: ${sinkLatestId}")

            if (sinkLatestId != null) {
              val srcLatestts = sparkSession.sql(s"select _ts from dfSrcTS where ${srcCutoffIdentifierField.get.toString} in " +
                s"('${sinkLatestId}')").first().get(0)
              logInfo(s"src_latest_ts: ${srcLatestts}")

              if (srcLatestts != null) {
                srcTSQueryStringCutOff = if (srcTSQueryString.contains("where")) {
                  s"${srcTSQueryString} and _ts <= ${srcLatestts}"
                }
                else {
                  s"${srcTSQueryString} where _ts <= ${srcLatestts}"
                }
              }
            }
            logInfo(s"srcTSQueryStringCutOff: ${srcTSQueryStringCutOff}")
          }

          dfSrcTSResults = sparkSession.sql(srcTSQueryStringCutOff).cache()
          dfSrcTSResults.count()
          dfSrcTSResults.toJSON.collect()
        }

        var dfSinkTSResults = sparkSession.emptyDataFrame
        val (sinkTSResults, sinkTSExecDuration) = execDuration {
          dfSinkTSResults = sparkSession.sql(queryString.replace("from c", "from dfSinkTS")
            .replace("FROM c", "from dfSinkTS").replace("from c", "from dfSinkTS"))
            .cache()
          dfSinkTSResults.count()
          dfSinkTSResults.toJSON.collect()
        }

        var dfSinkASResults = sparkSession.emptyDataFrame
        val (sinkASResult, sinkASExecDuration) = execDuration {
          dfSinkASResults = sparkSession.sql(queryString.replace("from c", "from dfSinkAS")
            .replace("FROM c", "from dfSinkAS").replace("from c", "from dfSinkAS"))
            .cache()
          dfSinkASResults.count()
          dfSinkASResults.toJSON.collect()
        }

        val endTimeLocal = DateTime.now().toString()

        val overallResult = if ((dfSrcTSResults.exceptAll(dfSinkTSResults).count() == 0 &&  dfSinkTSResults.exceptAll(dfSrcTSResults).count() == 0
          && dfSinkTSResults.exceptAll(dfSinkASResults).count() == 0 && dfSinkASResults.exceptAll(dfSinkTSResults).count() == 0)) true else false

        val queryResults = Seq(QueryResultsArray(queryString, "custom", startTimeLocal, endTimeLocal, srcTSResults, srcTSExecDuration, sinkTSResults, sinkTSExecDuration, sinkASResult, sinkASExecDuration, overallResult)).toDF()
        queryResults.createOrReplaceTempView("queryResults")
        val metricsInsertScript = s"insert into ${resultsDBName}.${resultsTableName} select * from queryResults"
        sparkSession.sql(metricsInsertScript)
      } catch {
       case exception: Exception => throw new RuntimeException(s"Execution failed for query: $queryString." +
          exception.printStackTrace())
      } finally {
        sparkSession.catalog.clearCache()
      }
    }

  private def createResultsTableIfNotExists() : Unit = {
    val metricsTableDDL = s"""CREATE TABLE IF NOT EXISTS ${resultsDBName}.${resultsTableName}(
                              query String,
                              queryType String,
                              startTime String,
                              endTime String,
                              srcTSResult String,
                              srcTSExecDuration int,
                              sinkTSResult String,
                              sinkTSExecDuration int,
                              sinkASResult String,
                              sinkASExecDuration int,
                              overallResult String)"""
    sparkSession.sql(metricsTableDDL)
  }
}

case class QueryResultsNumbers(query: String,
                               queryType: String,
                               startTime: String,
                               endTime: String,
                               srcTSResult: Long,
                               srcTSExecDuration: Long,
                               sinkTSResult: Long,
                               sinkTSExecDuration: Long,
                               sinkASResult: Long,
                               sinkASExecDuration: Long,
                               overallResult: Boolean)

case class QueryResultsArray(query: String,
                                 queryType: String,
                                 startTime: String,
                                 endTime: String,
                                 srcTSResult: Array[(String)],
                                 srcTSExecDuration: Long,
                                 sinkTSResult: Array[(String)],
                                 sinkTSExecDuration: Long,
                                 sinkASResult: Array[(String)],
                                 sinkASExecDuration: Long,
                                 overallResult: Boolean)

object AssessmentExecutor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("QueryExecution")
    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()
    val optionParser = ToolkitOptionsParser()
    val argValues = optionParser.parseOptions(args)
    val cosmosColsToDrop = Seq("_attachments", "_etag", "_lsn", "_rid", "_self")

    val srcTSConfig = Map(
      "spark.cosmos.accountendpoint" -> s"${argValues.srcCosmosEndpoint}",
      "spark.cosmos.accountkey" -> s"${argValues.srcCosmosAccountKey}",
      "spark.cosmos.database" -> s"${argValues.srcCosmosDatabaseName}",
      "spark.cosmos.container" -> s"${argValues.srcCosmosCollectionName}"
    )
    val dfSrcTS = sparkSession.read.format(config.cosmosOLTP).options(srcTSConfig).load().drop(cosmosColsToDrop: _*)
    dfSrcTS.createOrReplaceTempView("dfSrcTS")

    val sinkConfig = Map(
      "spark.cosmos.accountendpoint" -> s"${argValues.sinkCosmosEndpoint}",
      "spark.cosmos.accountkey" -> s"${argValues.sinkCosmosAccountKey}",
      "spark.cosmos.PreferredRegions" -> s"${argValues.sinkCosmosRegion}",
      "spark.cosmos.database" -> s"${argValues.sinkCosmosDatabaseName}",
      "spark.cosmos.container" -> s"${argValues.sinkCosmosCollectionName}"
    )
    val dfSinkTS = sparkSession.read.format(config.cosmosOLTP).options(sinkConfig).load().drop(cosmosColsToDrop: _*)
    dfSinkTS.createOrReplaceTempView("dfSinkTS")
    val dfSinkAS = sparkSession.read.format(config.cosmosOLAP).options(sinkConfig).load().drop(cosmosColsToDrop: _*)
    dfSinkAS.createOrReplaceTempView("dfSinkAS")

    AssessmentExecutor(sparkSession,
      argValues.srcCosmosEndpoint,
      argValues.srcCosmosAccountKey,
      argValues.srcCosmosRegion,
      argValues.srcCosmosDatabaseName,
      argValues.srcCosmosCollectionName,
      argValues.sinkCosmosEndpoint,
      argValues.sinkCosmosAccountKey,
      argValues.sinkCosmosRegion,
      argValues.sinkCosmosDatabaseName,
      argValues.sinkCosmosCollectionName,
      argValues.resultsDBName,
      argValues.resultsTableName,
      argValues.skipCustomQueryExecution,
      argValues.queriesToRun,
      argValues.srcCutoffIdentifierField,
      argValues.cellComparisonWhereClause,
      argValues.cellComparisonRecordsLimit,
      argValues.cellComparisonSortField,
      dfSrcTS: DataFrame,
      dfSinkTS: DataFrame,
      dfSinkAS: DataFrame).execute()
  }

  def apply(sparkSession: SparkSession
            , srcCosmosEndpoint: String
            , srcCosmosAccountKey: String
            , srcCosmosRegion: String
            , srcCosmosDatabaseName: String
            , srcCosmosCollectionName: String
            , sinkCosmosEndpoint: String
            , sinkCosmosAccountKey: String
            , sinkCosmosRegion: String
            , sinkCosmosDatabaseName: String
            , sinkCosmosCollectionName: String
            , resultsDBName: String
            , resultsTableName: String
            , skipCustomQueryExecution: Boolean
            , queriesToRun: String
            , srcCutoffIdentifierField: Option[String]
            , cellComparisonWhereClause: Option[String]
            , cellComparisonRecordsLimit: Option[String]
            , cellComparisonSortField: Option[String]
            , dfSrcTS: DataFrame
            , dfSinkTS: DataFrame
            , dfSinkAS: DataFrame): AssessmentExecutor =
    new AssessmentExecutor(sparkSession
      , srcCosmosEndpoint
      , srcCosmosAccountKey
      , srcCosmosRegion
      , srcCosmosDatabaseName
      , srcCosmosCollectionName
      , sinkCosmosEndpoint
      , sinkCosmosAccountKey
      , sinkCosmosRegion
      , sinkCosmosDatabaseName
      , sinkCosmosCollectionName
      , resultsDBName
      , resultsTableName
      , skipCustomQueryExecution
      , queriesToRun
      , srcCutoffIdentifierField
      , cellComparisonWhereClause
      , cellComparisonRecordsLimit
      , cellComparisonSortField
      , dfSrcTS
      , dfSinkTS
      , dfSinkAS)
}
