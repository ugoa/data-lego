
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.{DataFrame, TempView}

/**
  * Convert temp view to DataFrame.
  */
final class TempView2DataFrame(node: Node) extends Statement(node) {
  override lazy val parentOutputTypes: Vector[String] = Vector(TempView)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val snippet: String = {

    val parentTempView = parentOutputs(0)
    val dataFrameOutput = selfOutputs(0)

    s"""
       |val $dataFrameOutput = spark.sql("select * from $parentTempView")
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
