
package models.sparkworkflow.statement.filtering

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.Statement

/**
  * Project a DataFrame.
  */
final class Projection(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  /**
    * projection_columns: "origin_col:renamed_col,origin_col:renamed_col"
    */
  override lazy val paramKeys: Vector[String] = Vector("projection_columns")

  private lazy val projectionColumns = get(paramKeys.head)

  private def projectionColumnsValid_? : Boolean = {
    if (projectionColumns == "") return false
    try {
      projectionColumns.split(",").foreach { colPair =>
        colPair.split(":").map(_.trim) match {
          case Array(indicator, value) => true
          case Array(indicator, value, renamedCol) => true
        }
      }
      true
    } catch { case _: Exception => false }
  }

  require(
    projectionColumnsValid_?,
    s"""$concreteStatement: projection_columns cannot be empty
       |and must be format of 'name:city,index:9' or
       |'name:city:renamed_col1,index:9:renamed_col2' indicating no column to be renamed.
       |""".stripMargin
  )

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val Vector(dataFrameOutput, transformerOutput) = selfOutputs

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.sql.functions.col
       |import org.apache.spark.sql.Column
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |
       |  def getColumnName(indicator: String, value: String): String = {
       |
       |    val allColumns: Array[String] = df.schema.fieldNames
       |    if (indicator == "index") allColumns(value.toInt)
       |    else if (indicator == "name") value
       |    else value
       |  }
       |
       |  val renamePairs: Array[(String, String)] = "$projectionColumns".split(",").map { colPair =>
       |    colPair.split(":").map(_.trim) match {
       |      case Array(indicator, value) =>
       |        val colName = getColumnName(indicator, value)
       |        (colName, colName)
       |      case Array(indicator, value, renamedCol) =>
       |        val colName = getColumnName(indicator, value)
       |        (colName, renamedCol)
       |    }
       |  }
       |
       |  val selectedColumns: Array[Column] =
       |    renamePairs.map { case (origin, renamed) => col(origin).as(renamed) }
       |  df.select(selectedColumns: _*)
       |}
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
