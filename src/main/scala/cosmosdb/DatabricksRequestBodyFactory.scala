package cosmosdb

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Factory class of getting a databricks Request
 * @param jsonMapper
 */
class DatabricksRequestBodyFactory(jsonMapper: ObjectMapper with ScalaObjectMapper) {

  def getDatabricksRequest(job: Job, newCluster: Map[String, Object],
                           existingClusterId: String): String = {

    val applicationArgs: String = jsonMapper.writeValueAsString(job.applicationArgs)
    val newClusterString: String = jsonMapper.writeValueAsString(newCluster)

    var requestBody = s"""{
                         |  "run_name": "${job.name}",
                         |  "libraries": [
                         |    {
                         |      "jar": "${job.jarFilePath}"
                         |    }
                         |  ],
                         |  "spark_jar_task": {
                         |    "main_class_name": "${job.className}",
                         |    "parameters": $applicationArgs
                         |  },""".stripMargin

    if (existingClusterId != null) {
      requestBody +=
        s"""
           |  "existing_cluster_id": "$existingClusterId"
           |}""".stripMargin
    } else {
      requestBody +=
        s"""
           |  "new_cluster": $newClusterString
           |}""".stripMargin
    }
    requestBody
  }
}

object DatabricksRequestBodyFactory {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_NULL)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def apply(): DatabricksRequestBodyFactory = new DatabricksRequestBodyFactory(mapper)
}

