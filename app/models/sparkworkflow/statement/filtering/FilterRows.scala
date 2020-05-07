
package models.sparkworkflow.statement.filtering

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.Statement

/**
  * Filter rows by given codition, which must be valid WHERE clause of Spark SQL.
  */
final class FilterRows(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector("condition")

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)
    val conditionSnippet = get("condition")

    s"""
       |import org.apache.spark.sql.DataFrame
       |def $scopingMethod(df: DataFrame): DataFrame = {
       | df.where("$conditionSnippet")
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

