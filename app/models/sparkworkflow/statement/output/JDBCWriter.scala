
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame

object JDBCWriter {

  val ParamKeys: Vector[String] =
    Vector("host", "port", "database", "schema", "db_user", "db_password", "table", "properties") ++
    Vector("save_mode")
}

/**
  * Abstract class for write DataFrame to various JDBC datastore.
  */
abstract class JDBCWriter(node: Node) extends Statement(node) {

  def driver: String

  override final lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override final lazy val selfOutputTypes: Vector[String] = Vector()

  override final lazy val paramKeys: Vector[String] = JDBCWriter.ParamKeys

  requireNonEmpty(paramKeys.filterNot(Set("schema", "properties", "port", "database")))

  override final lazy val snippet: String = {

    val parentDataframe = parentOutputs(0)

    val p: JDBCWriterParams = getJDBCWriterParams

    s"""
       |def $scopingMethod() = {
       |
       |  val url = "jdbc:$driver://${p.formattedUrl}"
       |  val props = new java.util.Properties()
       |
       |  props.setProperty("user", "${p.dbUser}")
       |  props.setProperty("password", "${p.dbPassword}")
       |
       |  val propStr = "${p.properties}".trim
       |  if (propStr.nonEmpty) {
       |    propStr.split(",").foreach { propPair =>
       |      val Array(key, value) = propPair.split("=")
       |      props.setProperty(key, value)
       |    }
       |  }
       |
       |  $parentDataframe
       |    .coalesce(4)
       |    .write
       |    .mode("${p.saveMode}")
       |    .jdbc(url, "${p.formattedTable}", props)
       |}
       |$scopingMethod()
       |""".stripMargin
  }

  protected final def getJDBCWriterParams: JDBCWriterParams = {
    val Vector(host, port, database, schema, dbUser, dbPassword, table, properties, saveMode) =
      JDBCWriter.ParamKeys.map(get)

    JDBCWriterParams(host, port, database, schema, dbUser, dbPassword, table, properties, saveMode.toLowerCase)
  }

  case class JDBCWriterParams(
      private val host: String,
      private val port: String,
      private val database: String,
      private val schema: String,
      dbUser: String,
      dbPassword: String,
      private val table: String, // Only allow to use formattedTable
      properties: String,
      private val saveModeInput: String) {

    val formattedUrl: String = {
      val formattedHost = if (host.endsWith("/")) host.dropRight(1) else host
      val url1 = if (port.isEmpty) formattedHost else s"$formattedHost:$port"
      if (database.isEmpty) url1 else s"$url1/$database"
    }

    val formattedTable: String = if (schema.isEmpty) table else s"$schema.$table"

    val saveMode: String = if (saveModeInput.nonEmpty) saveModeInput else "default"

    require(
      Set("overwrite", "append", "default").contains(saveMode),
      s"$concreteStatement: Unknow save mode `$saveMode`"
    )
  }
}
