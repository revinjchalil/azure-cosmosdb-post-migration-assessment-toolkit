package cosmosdb

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

trait OptionsParser {

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def parse(pathToOptionsJson: String): ParsedOptions

  def parseJson[A](pathToOptionsJson: String)(implicit m: Manifest[A]): A = {
    if (pathToOptionsJson == null) {
      throw new IllegalArgumentException("The path to request json is required.")
    }
    val requestJson = scala.io.Source.fromFile(pathToOptionsJson).mkString
    mapper.readValue[A](requestJson)
  }

}

trait ParsedOptions {

  def jarFilePath: String

  def applicationArgs: ToolkitGeneratorOptions

  def conf: Map[String, String]
}

trait RunnerOptions {

  def jarFilePath: String

  def applicationArgs: Array[String]

  def conf: Map[String, String]
}
