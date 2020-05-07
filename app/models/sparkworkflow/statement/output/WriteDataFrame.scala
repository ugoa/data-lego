
package models.sparkworkflow.statement.output

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.SparkWorkflow.Node

/**
  * Write dataframe to Hive.
  */
final class WriteDataFrame(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector()

  override lazy val paramKeys: Vector[String] = Vector("hive_table", "hive_schema")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val dataFrame = parentOutputs(0)
    val Vector(targetTable, schema) = paramKeys.map(get)

    s"""
       |$dataFrame.write.format("parquet").mode("overwrite").saveAsTable("$schema.$targetTable")
       |""".stripMargin
  }
}
