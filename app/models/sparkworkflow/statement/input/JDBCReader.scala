
package models.sparkworkflow.statement.input

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.sparkworkflow.statement.Statement

object JDBCReader {

  val ParamKeys: Vector[String] =
    Vector("host", "port", "database", "schema", "db_user", "db_password", "table", "properties") ++
    Vector("lower_bound", "upper_bound", "partition_column", "num_partitions")

  val DefaultPartitionNum = 1
}

/**
  * Abstract class for reading data from various JDBC sources to produce a DataFrame.
  */
abstract class JDBCReader(node: Node) extends Statement(node) {

  /**
    * Expect the driver jar being included in the jdbc-driver-assembly.jar
    * @return the exact driver protocol used in the url string. e.g.
    *         "jdbc:<driver>://host:port/db?..."
    */
  def driver: String

  override final lazy val parentOutputTypes: Vector[String] = Vector()

  override final lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override final lazy val paramKeys: Vector[String] = JDBCReader.ParamKeys

  override final lazy val snippet: String = {

    val dataFrameOutput = selfOutputs(0)

    val p: JDBCReaderParams = getJDBCReaderParams

    val reading: String = if (p.partitionColumn.isEmpty) {
      s"""spark.read.jdbc(url, "${p.formattedTable}", props)"""
    } else {
      s"""
         |spark.read.jdbc(
         |  url,
         |  "${p.formattedTable}",
         |  "${p.partitionColumn}",
         |  ${p.lowerBound}.toLong,
         |  ${p.upperBound}.toLong,
         |  ${p.numPartitions},
         |  props
         |)
       """.stripMargin
    }

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
       |      props.put(key, value)
       |    }
       |  }
       |
       |  $reading
       |}
       |
       |val $dataFrameOutput = $scopingMethod()
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }

  protected final def getJDBCReaderParams: JDBCReaderParams = {
    val Vector(host, port, database, schema, dbUser, dbPassword, table, properties, lowerBound,
      upperBound, partitionCol, numPartitions) = JDBCReader.ParamKeys.map(get)

    JDBCReaderParams(
      host,
      port,
      database,
      schema,
      dbUser,
      dbPassword,
      table,
      properties,
      lowerBound,
      upperBound,
      partitionCol,
      numPartitions
    )
  }

  case class JDBCReaderParams(
      private val host: String,
      private val port: String,
      private val database: String,
      private val schema: String,
      dbUser: String,
      dbPassword: String,
      private val table: String,
      properties: String,
      lowerBound: String,
      upperBound: String,
      partitionColumn: String,
      private val numPartitionsInput: String) {

    if (partitionColumn.nonEmpty) {
      checkBoundParams()
    }

    val formattedUrl: String = {
      val formattedHost = if (host.endsWith("/")) host.dropRight(1) else host
      val url1 = if (port.isEmpty) formattedHost else s"$formattedHost:$port"
      if (database.isEmpty) url1 else s"$url1/$database"
    }

    val formattedTable: String = if (schema.isEmpty) table else s"$schema.$table"

    val numPartitions: Int = {
      if (numPartitionsInput.isEmpty) {
        JDBCReader.DefaultPartitionNum
      } else {
        val i = numPartitionsInput.toInt
        if (i > 20) 20 else if (i < 1) JDBCReader.DefaultPartitionNum else i
      }
    }

    private def checkBoundParams(): Unit = {
      require(
        lowerBound.nonEmpty && upperBound.nonEmpty,
        s"$concreteStatement: lowerbound and upperbound must has value"
      )

      require(
        try { lowerBound.toInt; true } catch { case _: Exception => false },
        s"$concreteStatement: Expect vaule of lowerbound to be integer. Got: '$lowerBound'"
      )

      require(
        try { upperBound.toInt; true } catch { case _: Exception => false },
        s"$concreteStatement: Expect vaule of lowerbound to be integer. Got: '$upperBound'"
      )
    }
  }
}
