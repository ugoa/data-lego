
package models.sparkworkflow.statement.input

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.Transformer
import models.SparkWorkflow.Node


/**
  * Read ML model from assigned source as a PipelineModel.
  */
final class ReadModel(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(Transformer)

  override lazy val paramKeys: Vector[String] = Vector("source")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {
    val transformerOutput = selfOutputs(0)

    val pplModelVal = "pplModel" + node.orderId
    val source = get("source")
    s"""
       |val $pplModelVal = org.apache.spark.ml.PipelineModel.load("$source")
       |val $transformerOutput = $pplModelVal.transform _
       |""".stripMargin
  }
}
