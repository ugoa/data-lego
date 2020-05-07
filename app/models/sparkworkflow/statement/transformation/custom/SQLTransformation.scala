
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.sparkworkflow.statement.Statement

/**
  * Take Spark SQL expression input to process transformation.
  */
final class SQLTransformation(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector("dataframe_id", "expression")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val dataFrameOutput = selfOutputs(0)
    val Vector(dfTempView, expression) = paramKeys.map(get)

    s"""
       |$parentDataFrame.createOrReplaceTempView("$dfTempView")
       |val $dataFrameOutput = spark.sql("$expression")
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
