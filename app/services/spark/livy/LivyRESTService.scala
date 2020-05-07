
package services.spark.livy

import javax.inject.Inject

import scalaj.http._

import play.api.libs.json.{JsValue, Json}

import services.spark.livy.LivyRESTService._
import utils.Helper.escape

object LivyRESTService {

  val DefaultPreviewSize = 100

  object CodeType {
    val Spark = "spark"
    val PySpark = "pyspark"
    val SparkR = "sparkr"
    val SQL = "sql"

    val SupportedCodeTypes = Set(Spark, PySpark, SparkR, SQL)
  }
}

/**
  * Livy REST service that interact with Spark Livy web server.
  */
class LivyRESTService @Inject()(config: play.Configuration) {

  private val LivyServerURL = s"${config.getString("livy.host")}:${config.getString("livy.port")}"

  private var _sessionId: Int = -1

  def setSession(id: Int): Int = {
    _sessionId = id
    _sessionId
  }

  def session: Int = _sessionId

  /**
    * Create a Livy session. Assign the JDBC jars to support JDBCReader and JDBCWriter node.
    */
  def createSession(): JsValue = {
    val data =
      s"""
         |{
         |  "conf": {
         |    "spark.jars": "${config.getString("JDBCDriverAssembleJarPath")}",
         |    "spark.driver.extraClassPath": "${config.getString("JDBCDriverAssembleJarName")},${config.getString("ESHadoopJarPath")}"
         |  }
         |}
         |""".stripMargin.replaceAll("\n", " ")

    val respJson = post(s"$LivyServerURL/sessions", data)
    _sessionId = (respJson \ "id").as[Int]
    respJson
  }


  def deleteSession(sessionId: Int): String = {
    val url = s"$LivyServerURL/sessions/$sessionId"

    delete(url)
  }

  def getSessionStatus(sessionId: Int): JsValue = {
    val url = s"$LivyServerURL/sessions/$sessionId"

    get(url)
  }

  def postStatement(sessionId: Int, code: String, codeType: String = CodeType.Spark): JsValue = {
    require(
      CodeType.SupportedCodeTypes.contains(codeType),
      s"Code type `$codeType` of $code is not supported in livy session $sessionId"
    )

    val url = s"$LivyServerURL/sessions/$sessionId/statements"

    val data = s""" { "code": "${escape(code)}", "kind": "$codeType" } """

    post(url, data)
  }

  /**
    * Get top n rows of a dataframe determined by the dataframe variable name.(val)
    *
    * @param sessionId the session id.
    * @param dfVal the dataframe variable name.
    * @param size the number of rows to return.
    */
  def getDFPreview(sessionId: Int, dfVal: String, size: Int = DefaultPreviewSize): JsValue = {

    val newColsVal = dfVal + "NewCols"
    val codeForPreview =
      s"""
         |val $newColsVal = $dfVal.schema.map(field => field.name + "_" + field.dataType.toString)
         |$dfVal.toDF($newColsVal: _*).show($size, false)
         |""".stripMargin

    postStatement(sessionId, codeForPreview, CodeType.Spark)
  }

  def getStatementStatus(sessionId: Int, statementId: Int): JsValue = {
    val url = s"$LivyServerURL/sessions/$sessionId/statements/$statementId"

    get(url)
  }

  private def get(url: String): JsValue = {

    play.api.Logger.info(s"GET $url")
    val request: HttpRequest = Http(url).headers(headerParams)

    Json.parse(request.asString.body)
  }

  private def delete(url: String): String = {
    val request: HttpRequest = Http(url).headers(headerParams).method("DELETE")

    request.asString.body
  }

  private def post(url: String, data: String = "{}"): JsValue = {
    play.api.Logger.info(s"POST $url $data")
    val request: HttpRequest = Http(url).headers(headerParams).postData(data)
    Json.parse(request.asString.body)
  }

  private lazy val headerParams = Seq(
    "X-Requested-By" -> s"${config.getString("livy.user")}",
    "Content-Type" -> "application/json"
  )
}
