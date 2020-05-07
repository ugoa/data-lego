
package models.sparkworkflow.statement.action

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Estimator, Transformer}


/**
  * Execute the org.apache.spark.ml.Estimator#fit and org.apache.spark.ml.Transformer#transform APIs
  */
final class FitTransform(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Estimator)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val Vector(parentDataFrame, parentEstimator) = parentOutputs

    s"""
       |import org.apache.spark.sql.DataFrame
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |  ${extractAndResetColParamsSnippet(parentDataFrame, parentEstimator)}
       |  $parentEstimator.fit(df).transform(df)
       |}
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

