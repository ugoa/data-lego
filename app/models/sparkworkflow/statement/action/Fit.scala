
package models.sparkworkflow.statement.action

import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Estimator, Model, Transformer}
import models.SparkWorkflow.Node

/**
  * Execute the org.apache.spark.ml.Estimator#fit API
  */
final class Fit(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Estimator)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(Model, Transformer)

  override lazy val snippet: String = {

    val Vector(modelOutput, transformerOutput) = selfOutputs
    val Vector(parentDataFrame, parentEstimator) = parentOutputs

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.ml.Pipeline
       |import org.apache.spark.ml.PipelineModel
       |
       |def $scopingMethod(): PipelineModel = {
       |  ${extractAndResetColParamsSnippet(parentDataFrame, parentEstimator)}
       |  val ppl = new Pipeline().setStages(Array($parentEstimator))
       |  ppl.fit($parentDataFrame)
       |}
       |
       |val $modelOutput = $scopingMethod()
       |val $transformerOutput = $modelOutput.transform _
       |""".stripMargin
  }
}

