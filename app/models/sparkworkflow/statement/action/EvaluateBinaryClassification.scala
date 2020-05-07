
package models.sparkworkflow.statement.action

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Evaluator, MetricValue}
import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}

/**
  * Custom evaluation of the Binary Classification to produce the full metrics.
  * @see <a href="https://spark.apache.org/docs/2.2.0/mllib-evaluation-metrics.html#binary-classification">
  */
final class EvaluateBinaryClassification(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override def getDataFrameOutput(outputId: Int): Option[String] = {
    val valName = getOutputName(node.orderId, outputId, MetricValue)
    selfOutputs.find(_ == valName)
  }

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, Evaluator)

  override lazy val selfOutputTypes: Vector[String] = Vector(MetricValue, MetricValue, MetricValue, MetricValue)

  override lazy val paramKeys: Vector[String] = Vector("f_measure_beta_factor")

  requireNumeric(paramKeys)

  override lazy val snippet: String = {

    val Vector(parentDataframe, parentEvaluator) = parentOutputs
    val Vector(metricDF1, metricDF2, metricDF3, metricDF4) = selfOutputs

    val betaFactor = get(paramKeys(0))

    s"""
       |import org.apache.spark.sql.types.{StructField, StructType, DoubleType}
       |import org.apache.spark.sql.Row
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.rdd.RDD
       |import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
       |import org.apache.spark.ml.linalg.Vector
       |import org.apache.spark.sql.functions.col
       |import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
       |
       |def $scopingMethod(): (DataFrame, DataFrame, DataFrame, DataFrame) = {
       |  ${extractAndResetColParamsSnippet(parentDataframe, parentEvaluator)}
       |
       |  val rawPredictionCol: String = $parentEvaluator.getRawPredictionCol
       |  val labelCol: String = $parentEvaluator.getLabelCol
       |
       |  val scoreAndLabels: RDD[(Double, Double)] =
       |    $parentDataframe
       |      .select(
       |        col($parentEvaluator.getRawPredictionCol),
       |        col($parentEvaluator.getLabelCol).cast(DoubleType)
       |      )
       |      .rdd
       |      .map {
       |        case Row(rawPrediction: Vector, label: Double) => (rawPrediction(1), label)
       |        case Row(rawPrediction: Double, label: Double) => (rawPrediction, label)
       |      }
       |
       |  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
       |
       |  val metricDF1 = {
       |    val thresholdCol: String = "threshold"
       |    val precisionDF = metrics.precisionByThreshold.toDF(thresholdCol, "precision")
       |    val recallDF = metrics.recallByThreshold.toDF(thresholdCol, "recall")
       |    val fMeasureDF = metrics.fMeasureByThreshold($betaFactor.toDouble).toDF(thresholdCol, "f_measure")
       |
       |    precisionDF
       |      .join(recallDF, thresholdCol)
       |      .join(fMeasureDF, thresholdCol)
       |      .sort(desc(thresholdCol))
       |  }
       |
       |  val metricDF2 =
       |    sc
       |      .parallelize(Array((metrics.areaUnderROC, metrics.areaUnderPR)))
       |      .toDF("area_under_roc", "area_under_pr")
       |
       |  val metricDF3 = metrics.roc.toDF("false_positive_rate", "true_positive_rate")
       |
       |  val metricDF4 = metrics.pr.toDF("recall", "precision")
       |
       |  (metricDF1, metricDF2, metricDF3, metricDF4)
       |}
       |
       |val ($metricDF1, $metricDF2, $metricDF3, $metricDF4) = $scopingMethod()
       |
       |$metricDF1.cache()
       |$metricDF1.count()
       |$metricDF2.cache()
       |$metricDF2.count()
       |$metricDF3.cache()
       |$metricDF3.count()
       |$metricDF4.cache()
       |$metricDF4.count()
       |""".stripMargin
  }
}
