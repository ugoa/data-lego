
package models.sparkworkflow.statement.setoperation

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.SparkWorkflow.Node

/**
  * Sort dataframe in ascending/descending order by the given columns.
  */
final class Sort(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  /**
    * String of column(name or index) list to be sorted with each specified direction.
    * e.g. name:city:asc,index:8:desc,name:beds:desc
    */
  override lazy val paramKeys: Vector[String] = Vector("sort_columns")

  requireNonEmpty(paramKeys)

  require(
    sortColumnsValid_?,
    s"$concreteStatement: sort_columns must be in format like 'name:city:asc,index:8:desc,name:beds:desc'"
  )
  private def sortColumnsValid_? : Boolean = {
    val sortColumns = get("sort_columns")
    try {
      sortColumns.split(",").map { col =>
        val Array(indicator, value, order) = col.split(":").map(_.trim)
        Array("name", "index").contains(indicator) && Array("asc", "desc").contains(order)
      }.reduce(_ && _)
    } catch { case _: Exception => false }
  }

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val sortColumns = get("sort_columns")
    val dataFrameOutput = selfOutputs(0)

    s"""
       |def $scopingMethod() = {
       |  import org.apache.spark.sql.functions._
       |
       |  val allColumns: Array[String] = $parentDataFrame.schema.fieldNames
       |
       |  val columnsArray = "$sortColumns".split(",")
       |  val columnSortParams = columnsArray.map { columnConfig =>
       |    val Array(indicatedBy, value, direction) = columnConfig.split(":")
       |    val columnName = if (indicatedBy == "index") allColumns(value.toInt) else value
       |    if (direction == "asc") asc(columnName) else desc(columnName)
       |  }
       |  $parentDataFrame.sort(columnSortParams: _*)
       |}
       |val $dataFrameOutput = $scopingMethod()
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
