
package models.sparkworkflow.statement.machinelearning.modelevaluation

import models.sparkworkflow.statement.Statement.Output.Evaluator
import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.ml.evaluation.BinaryClassificationEvaluator">
  */
final class BinaryClassificationEvaluator(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "evaluation.BinaryClassificationEvaluator"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = BinaryClassificationEvaluator.ParamSetMethodMappings

  override lazy val selfOutputTypes: Vector[String] = Vector(Evaluator)
}

object BinaryClassificationEvaluator {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("metric_name", "setMetricName", "string"),
    ("label_column", "setLabelCol", "string"),
    ("raw_prediction_column", "setRawPredictionCol", "string")
  )
}
