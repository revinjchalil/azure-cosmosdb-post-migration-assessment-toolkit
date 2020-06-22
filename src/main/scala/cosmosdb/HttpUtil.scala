package cosmosdb

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging

/**
 * This util class is used for creating http requests.
 */
class HttpUtil extends LazyLogging {

  /**
   *
   * @param url Resource url
   * @param authHeader Auth header
   * @param retryCount Retry count
   * @return HttpURLConnection
   */
  def createGetConnection(url: String,
                          authHeader: String,
                          retryCount: Int = config.livyRetryCount): HttpURLConnection = {
    try {
      val httpURLConnection = httpGetConnection(url, authHeader)
      val responseCode = httpURLConnection.getResponseCode
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException("Unexpected response code " + responseCode)
      }
      httpURLConnection
    } catch {
      case e: Exception =>
        if (retryCount > 0) {
          logger.info(s"Retrying to create HTTP GET connection due to exception ${e.getMessage}")
          TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
          createGetConnection(url, authHeader, retryCount - 1)
        } else {
          throw e
        }
    }
  }

  /**
   *
   * @param url Resource url
   * @param authHeader Auth header
   * @param requestBody Request body
   * @param expectedResponseCode Expected response code
   * @param retryCount Retry count
   * @return
   */
  def createPostConnection(url: String,
                           authHeader: String,
                           requestBody: String,
                           expectedResponseCode: Int,
                           retryCount: Int = config.livyRetryCount): HttpURLConnection = {
    try {
      val httpURLConnection = httpPostConnection(url, authHeader)
      val outputStream = httpURLConnection.getOutputStream
      outputStream.write(requestBody.getBytes())
      outputStream.flush()
      outputStream.close()
      val responseCode = httpURLConnection.getResponseCode
      if (responseCode != expectedResponseCode) {
        throw new Exception("Unexpected response code " + responseCode)
      }
      httpURLConnection
    } catch {
      case e: Exception =>
        if (retryCount > 0) {
          logger.info(s"Retrying to create HTTP POST connection due to exception" +
            s" ${e.getMessage}")
          TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
          createPostConnection(url, authHeader, requestBody, expectedResponseCode, retryCount - 1)
        } else {
          throw e
        }
    }
  }

  private def httpGetConnection(url: String, authHeader: String): HttpURLConnection = {
    val httpURLConnection: HttpURLConnection =
      new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    httpURLConnection.setRequestProperty(
      "Accept",
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )
    httpURLConnection.setDoOutput(true)
    httpURLConnection.setRequestMethod("GET")
    httpURLConnection.setRequestProperty("X-Requested-By", "admin")
    httpURLConnection.setRequestProperty("Authorization", authHeader)
    httpURLConnection
  }

  private def httpPostConnection(url: String,
                                 authHeader: String): HttpURLConnection = {
    val httpURLConnection: HttpURLConnection =
      new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    httpURLConnection.setRequestProperty(
      "Accept",
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )
    httpURLConnection.setDoOutput(true)
    httpURLConnection.setRequestMethod("POST")
    httpURLConnection.setRequestProperty("Content-Type", "application/json")
    httpURLConnection.setRequestProperty("X-Requested-By", "admin")
    httpURLConnection.setRequestProperty("Authorization", authHeader)
    httpURLConnection
  }

  /**
   * Read response from an http connection
   *
   * @param httpURLConnection
   * @return Response as a string
   */
  def readResponse(httpURLConnection: HttpURLConnection): String = {
    try {
      val bufferedReader: BufferedReader =
        new BufferedReader(
          new InputStreamReader(httpURLConnection.getInputStream)
        )
      var line = bufferedReader.readLine()
      var output = line
      while (line.isEmpty) {
        output = output + s"\n$line"
        line = bufferedReader.readLine()
      }
      bufferedReader.close()
      logger.debug(s"The response is: $output")
      output
    } catch {
      case exception: Exception =>
        logger.error(
          s"Exception while reading the livy post response: ${exception.getStackTrace}"
        )
        ""
    }
  }

  /**
   * Get value of a json element
   *
   * @param response http response string
   * @param field json path
   * @return
   */
  def getJsonField(response: String, field: String): String = {
    var result: String = "-1"
    try {
      val mapper = new ObjectMapper
      result =
        mapper.readTree(response).at(field).asText()
    } catch {
      case exception: Exception =>
        logger.error("Exception while parsing the json response", exception)
    }
    result
  }
}

object HttpUtil {
  def apply(): HttpUtil = new HttpUtil()
}

