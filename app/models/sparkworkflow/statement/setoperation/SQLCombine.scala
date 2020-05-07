
package models.sparkworkflow.statement.setoperation

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.sparkworkflow.statement.Statement

/**
  * Run custom Spqrk SQL expression on 2 dataframes to produces one.
  */
final class SQLCombine(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes : Vector[String] = Vector(DataFrame, DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector("left_dataframe_id", "right_dataframe_id", "expression")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val Vector(df1, df2) = parentOutputs
    val dataFrameOutput = selfOutputs(0)
    val Vector(df1TempView, df2TempView, expression) = paramKeys.map(get)

    s"""
       |$df1.createOrReplaceTempView("$df1TempView")
       |$df2.createOrReplaceTempView("$df2TempView")
       |val $dataFrameOutput = spark.sql("$expression")
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
