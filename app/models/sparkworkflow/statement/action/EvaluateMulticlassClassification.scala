
package models.sparkworkflow.statement.action

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Evaluator, MetricValue}
import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}

/**
  * Custom evaluation of the Multiclass Classification to produce the full metrics.
  * @see <a href="https://spark.apache.org/docs/2.2.0/mllib-evaluation-metrics.html#multiclass-classification">
  */
final class EvaluateMulticlassClassification(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override def getDataFrameOutput(outputId: Int): Option[String] = {
    val valName = getOutputName(node.orderId, outputId, MetricValue)
    selfOutputs.find(_ == valName)
  }

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Evaluator)

  override lazy val selfOutputTypes: Vector[String] = Vector(MetricValue, MetricValue)

  override lazy val paramKeys: Vector[String] = Vector("f_measure_beta_factor")

  requireNumeric(paramKeys)

  override lazy val snippet: String = {

    val Vector(parentDataframe, parentEvaluator) = parentOutputs
    val Vector(metricDF1, metricDF2) = selfOutputs

    val betaFactor = get(paramKeys(0))

    s"""
       |import org.apache.spark.sql.types.DoubleType
       |import org.apache.spark.sql.Row
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.rdd.RDD
       |import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
       |import org.apache.spark.sql.functions.col
       |import org.apache.spark.mllib.evaluation.MulticlassMetrics
       |
       |def $scopingMethod(): (DataFrame, DataFrame) = {
       |  ${extractAndResetColParamsSnippet(parentDataframe, parentEvaluator)}
       |
       |  val predictionAndLabels: RDD[(Double, Double)] =
       |    $parentDataframe
       |      .select(
       |        col($parentEvaluator.getPredictionCol),
       |        col($parentEvaluator.getLabelCol).cast(DoubleType)
       |      )
       |      .rdd
       |      .map { case Row(prediction: Double, label: Double) => (prediction, label) }
       |
       |  val metrics = new MulticlassMetrics(predictionAndLabels)
       |
       |  val metricDF1: DataFrame = {
       |    val rawResult: Array[(Double, Double, Double, Double, Double, Double)] =
       |      metrics.labels.map { label =>
       |        val precision: Double = metrics.precision(label)
       |        val recall: Double = metrics.recall(label)
       |        val truePositiveRate: Double = metrics.truePositiveRate(label)
       |        val falsePositiveRate: Double = metrics.falsePositiveRate(label)
       |        val fMeasure: Double = metrics.fMeasure(label, $betaFactor.toDouble)
       |
       |        (label, precision, recall, truePositiveRate, falsePositiveRate, fMeasure)
       |      }
       |
       |    sc.parallelize(rawResult).toDF("label", "precision", "recall", "true_positive_rate", "false_positive_rate", "f_measure")
       |  }
       |
       |  val metricDF2: DataFrame = {
       |    val tuple: (String, Double, Double, Double, Double, Double, Double) = (
       |      metrics.confusionMatrix.toString.trim,
       |      metrics.accuracy,
       |      metrics.weightedTruePositiveRate,
       |      metrics.weightedFalsePositiveRate,
       |      metrics.weightedRecall,
       |      metrics.weightedPrecision,
       |      metrics.weightedFMeasure($betaFactor.toDouble)
       |    )
       |
       |    sc.parallelize(Array(tuple)).toDF(
       |      "confusion_matrix",
       |      "accuracy",
       |      "weighted_truepositive_rate",
       |      "weighted_falsepositive_rate",
       |      "weighted_recall",
       |      "weighted_precision",
       |      "weighted_f_measure"
       |    )
       |  }
       |
       |  (metricDF1, metricDF2)
       |}
       |
       |val ($metricDF1, $metricDF2) = $scopingMethod()
       |
       |$metricDF1.cache()
       |$metricDF1.count()
       |$metricDF2.cache()
       |$metricDF2.count()
       |""".stripMargin
  }
}
