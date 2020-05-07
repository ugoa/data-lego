
package services.datamart

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import scalaj.http.HttpRequest

import common.Field
import play.api.libs.json.{JsArray, JsNull, Json}

import utils.Helper.writeToFile

final class ODataDownloadTask(
    downloadRequest: HttpRequest,
    downloadedFilePath: String,
    odataVersion: String,
    newDataFields: Vector[Field]) extends Runnable {

  override def run(): Unit = {
    writeToFile(downloadedFilePath, parseToSparkJSONFormat(downloadRawOData()))
  }

  private def downloadRawOData(): String = {
    val request = downloadRequest.header("Accept", "application/json").timeout(10000, 60000)
    request.asString.body
  }

  /**
    * Convert raw odata payload to spark-valid json format string.
    * @param responseBody
    * {
    *   "d": {
    *     "results:" [
    *       {
    *         "_metadata": {...},
    *         "field1": "hello",
    *         "field2": 1,
    *         "field3": "date_string1"
    *       },
    *       {
    *         "_metadata": {...},
    *         "field1": "world",
    *         "field2": 2,
    *         "field3": "date_string2"
    *       }
    *       ......
    *     ]
    *   }
    * }
    * @return
    * { "field1": "hello", "field2": 1, "field3": "2017-01-24T05:34:56" }
    * { "field2": "world", "field2": 2, "field3": "2017-04-19T11:34:56" }
    * ......
    */
  private def parseToSparkJSONFormat(responseBody: String): String = {
    val path: Array[String] = odataVersion match {
      case "V1" => Array("d")
      case "V2" => Array("d", "results")
      case "V3" => Array("results")
      case "V4" => Array("value")
      case _ => Array("d", "results")
    }
    val jsonEntity = Json.parse(responseBody)
    val entityList = (jsonEntity.result /: path)(_ \ _)

    entityList.as[JsArray].value.map { entity =>

      val keyValues: String = newDataFields .map { field: Field =>
        val jsonValue = (entity \ field.originName).get

        val normalizedValue: String = jsonValue match {
          case JsNull => ""
          case _ => jsonValue.as[String]
        }

        val valueWithType = field.fieldType match {
          case "double" => normalizedValue.toDouble
          case "int" | "integer" | "number" => normalizedValue.toLong
          case "date" => s""""${toISO8601Format(normalizedValue, field.datePattern.get)}""""
          case _ => s""""${normalizedValue.toString}""""
        }

        s""""${field.name}": $valueWithType"""
      }.mkString(", ")

      s"{ $keyValues }"
    }.mkString("\n")
  }

  /**
    * @param time string represents a time, such as "2017-09-12", "3-9-2017 15:45:10"
    * @param pattern pattern of the time such as "YYYY-MM-DD HH:mm:ss"
    * @return A standard ISO8601 datetime format string. e.g. "2017-24T13:45:19"
    */
  private def toISO8601Format(time: String, pattern: String): String = {
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val localDateTime =
      if (dateOnly(pattern)) LocalDate.parse(time, formatter).atStartOfDay
      else LocalDateTime.parse(time, formatter)
    localDateTime.toString
  }

  private def dateOnly(pattern: String): Boolean = {
    // Ref: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
    val nonDateRelatedSymbols = Array("a", "h", "K", "k", "H", "m", "s", "S", "A", "N", "n")
    // It's NOT a Date string if any one of these symbols exists in the pattern
    !nonDateRelatedSymbols.exists(pattern.contains)
  }
}
