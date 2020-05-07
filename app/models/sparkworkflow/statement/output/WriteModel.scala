
package models.sparkworkflow.statement.output

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.Model
import models.SparkWorkflow.Node

/**
  * Write ML model to file path provided by user. Normally on HDFS.
  */
final class WriteModel(node: Node) extends Statement(node) {

  override lazy val paramKeys: Vector[String] = Vector("output_path", "overwrite")

  override lazy val parentOutputTypes: Vector[String] = Vector(Model)

  override lazy val selfOutputTypes: Vector[String] = Vector()

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val parentModelOutput = parentOutputs(0)
    val filePathToSave = get("output_path")

    val isOverwrite = get("overwrite").toLowerCase
    require(Set("true", "false").contains(isOverwrite), s"$isOverwrite MUST be true or false")

    val isOverwriteFn = if (isOverwrite == "true") ".overwrite" else ""

    s"""
       |$parentModelOutput.write$isOverwriteFn.save("$filePathToSave")
       |""".stripMargin
  }
}
