
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.{DataFrame, TempView}

/**
  * Produces a Spark temp view.
  */
final class DataFrame2TempView(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(TempView)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val tempViewOutput = selfOutputs(0)
    s"""
       |$parentDataFrame.createOrReplaceTempView("$tempViewOutput")
       |val $tempViewOutput: String = "$tempViewOutput"
       |""".stripMargin
  }
}
