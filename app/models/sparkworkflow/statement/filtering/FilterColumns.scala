
package models.sparkworkflow.statement.filtering

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.{MultiColumnParamsInfo, Statement}
import models.sparkworkflow.statement.MultiColumnParamsInfo.MultiColumnParamKeys

/**
  * Filter columns by given column names/indices/types.
  */
final class FilterColumns(node: Node) extends Statement(node) with MultiColumnParamsInfo {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = MultiColumnParamKeys

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)

    val Vector(filterMode, colNames, colTypes, indexRanges) = MultiColumnParamKeys.map(get)

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.sql.functions.col
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |
       |  $ExtractColumnsMethodDefinition
       |  val selectedColumns: Array[String] = $ExtractColumnsOp(df, "$filterMode", "$colNames", "$colTypes", "$indexRanges")
       |  df.select(selectedColumns.map(col): _*)
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

