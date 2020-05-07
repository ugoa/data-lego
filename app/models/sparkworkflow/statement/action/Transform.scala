
package models.sparkworkflow.statement.action

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output._
import models.SparkWorkflow.Node

/**
  * Transform a DataFrame by the parent transformer.
  */
final class Transform(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val snippet: String = {

    val dataFrameOutput = selfOutputs(0)
    val Vector(parentDataFrame, parentTransformer) = parentOutputs

    s"""
       |import org.apache.spark.sql.DataFrame
       |val $scopingMethod: DataFrame => DataFrame = $parentTransformer
       |val $dataFrameOutput: DataFrame = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
