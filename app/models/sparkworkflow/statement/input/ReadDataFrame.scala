
package models.sparkworkflow.statement.input

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.SparkWorkflow.Node

/**
  * Read data from Hive as a DataFrame.
  */
final class ReadDataFrame(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector("hive_table", "hive_schema")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val dataFrameOutput = selfOutputs(0)
    val Vector(hiveTable, hiveSchema) = paramKeys.map(get)

    s"""
       |val $dataFrameOutput = spark.sql("select * from $hiveSchema.$hiveTable")
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
