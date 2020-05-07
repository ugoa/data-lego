
package models.sparkworkflow.statement.setoperation

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.SparkWorkflow.Node

/**
  * Join two dataframes
  */
final class Join(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  /**
    * left_cols: "name:city,index:9"
    * right_cols: "name:city,index:9"
    */
  override lazy val paramKeys: Vector[String] = Vector("left_cols", "right_cols", "join_type", "left_prefix", "right_prefix")

  override lazy val snippet: String = {

    val Vector(df1, df2) = parentOutputs
    val dataFrameOutput = selfOutputs(0)

    val Vector(leftCols, rightCols, joinType) = paramKeys.take(3).map(get)
    val Vector(leftPrefix, rightPrefix) = paramKeys.takeRight(2).map { key =>
      val prefix = get(key)
      if (prefix.endsWith("_")) prefix else prefix + "_"
    }

    require(
      leftCols.split(",").length == rightCols.split(",").length,
      s"$concreteStatement: Amount of left/right join columns must be equal"
    )

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.sql.functions.col
       |import org.apache.spark.sql.Column
       |
       |def $scopingMethod() = {
       |
       |  def addPrefixToDF(df: DataFrame, prefix: String): DataFrame = {
       |    val schema = df.schema
       |    val originCols: Array[String] = df.schema.fieldNames
       |    val prefixedCols = originCols.map(name => col(name).as(s"$$prefix$$name"))
       |    df.select(prefixedCols: _*)
       |  }
       |
       |  val df1 = addPrefixToDF($df1, "$leftPrefix")
       |  val df2 = addPrefixToDF($df2, "$rightPrefix")
       |
       |  def fetchColNames(df: DataFrame, colsStr: String, prefix: String): Array[String] = {
       |    val schema = df.schema
       |    val allColumns: Array[String] = df.schema.fieldNames
       |    colsStr.split(",").map { col =>
       |      val Array(indicator, value) = col.trim.split(":").map(_.trim)
       |      val colName = if (indicator == "name") value else allColumns(value.toInt)
       |      if (prefix.isEmpty) colName else prefix + colName
       |    }
       |  }
       |
       |  val leftCols: Array[String] = fetchColNames($df1, "$leftCols", "$leftPrefix")
       |  val rightCols: Array[String] = fetchColNames($df2, "$rightCols", "$rightPrefix")
       |  val pairs = leftCols zip rightCols
       |
       |  val column: Column =
       |    pairs.map { case (left, right) => df1.col(left) === df2.col(right) }.reduceLeft(_ && _)
       |
       |  df1.join(df2, column, "$joinType")
       |}
       |
       |val $dataFrameOutput = $scopingMethod()
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
