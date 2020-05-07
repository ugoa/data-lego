
package models.sparkworkflow.statement.action

import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Evaluator, MetricValue}
import models.SparkWorkflow.Node

/**
  * Execute the org.apache.spark.ml.evaluation.Evaluator#evaluate API.
  */
final class Evaluate(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override def getDataFrameOutput(outputId: Int): Option[String] = {
    val valName = getOutputName(node.orderId, outputId, MetricValue)
    selfOutputs.find(_ == valName)
  }

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Evaluator)

  override lazy val selfOutputTypes: Vector[String] = Vector(MetricValue)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val snippet: String = {

    val outputMetricValue = selfOutputs(0)
    val Vector(parentPredictionDF, parentEvaluator) = parentOutputs

    s"""
       |import org.apache.spark.sql.types.{StructField, StructType, DoubleType}
       |import org.apache.spark.sql.Row
       |import org.apache.spark.sql.DataFrame
       |
       |def $scopingMethod(): DataFrame = {
       |  ${extractAndResetColParamsSnippet(parentPredictionDF, parentEvaluator)}
       |
       |  val metricValue: Double = $parentEvaluator.evaluate($parentPredictionDF)
       |  sc.parallelize(Array((metricValue))).toDF("metric")
       |}
       |val $outputMetricValue = $scopingMethod()
       |$outputMetricValue.cache()
       |$outputMetricValue.count()
       |""".stripMargin
  }
}
