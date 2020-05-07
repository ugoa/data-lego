
package models.sparkworkflow.statement.action

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Evaluator, MetricValue}
import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}

/**
  * Custom evaluation of the Regression to produce the full metrics.
  * @see <a href="https://spark.apache.org/docs/2.2.0/mllib-evaluation-metrics.html#regression-model-evaluation">
  */
final class EvaluateRegression(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override def getDataFrameOutput(outputId: Int): Option[String] = {
    val valName = getOutputName(node.orderId, outputId, MetricValue)
    selfOutputs.find(_ == valName)
  }

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Evaluator)

  override lazy val selfOutputTypes: Vector[String] = Vector(MetricValue)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val snippet: String = {

    val Vector(parentDataframe, parentEvaluator) = parentOutputs
    val metricsDF = selfOutputs(0)

    s"""
       |import org.apache.spark.sql.types.DoubleType
       |import org.apache.spark.sql.Row
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.rdd.RDD
       |import org.apache.spark.ml.evaluation.RegressionEvaluator
       |import org.apache.spark.sql.functions.col
       |import org.apache.spark.mllib.evaluation.RegressionMetrics
       |
       |def $scopingMethod(): DataFrame = {
       |  ${extractAndResetColParamsSnippet(parentDataframe, parentEvaluator)}
       |
       |  val predictionCol: String = $parentEvaluator.getPredictionCol
       |  val labelCol: String = $parentEvaluator.getLabelCol
       |
       |  val predictionAndLabels: RDD[(Double, Double)] =
       |    $parentDataframe
       |      .select(
       |        col($parentEvaluator.getPredictionCol).cast(DoubleType),
       |        col($parentEvaluator.getLabelCol).cast(DoubleType)
       |       )
       |      .rdd
       |      .map {
       |        case Row(prediction: Double, label: Double) => (prediction, label)
       |      }
       |  val metrics = new RegressionMetrics(predictionAndLabels)
       |
       |  val rawResult: (Double, Double, Double, Double, Double) = (
       |    metrics.explainedVariance,
       |    metrics.meanAbsoluteError,
       |    metrics.meanSquaredError,
       |    metrics.rootMeanSquaredError,
       |    metrics.r2
       |  )
       |
       |  sc.parallelize(Array(rawResult)).toDF("explained_variance", "mean_absolute_error", "mean_squared_error", "root_mean_squared_error", "r2")
       |}
       |
       |val $metricsDF = $scopingMethod()
       |$metricsDF.cache()
       |$metricsDF.count()
       |""".stripMargin
  }
}