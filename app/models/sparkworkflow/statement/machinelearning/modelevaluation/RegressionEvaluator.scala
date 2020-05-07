
package models.sparkworkflow.statement.machinelearning.modelevaluation

import models.sparkworkflow.statement.Statement.Output.Evaluator
import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.ml.evaluation.RegressionEvaluator">
  */
final class RegressionEvaluator(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "evaluation.RegressionEvaluator"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = RegressionEvaluator.ParamSetMethodMappings

  override lazy val selfOutputTypes: Vector[String] = Vector(Evaluator)
}

object RegressionEvaluator {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("metric_name", "setMetricName", "string"),
    ("label_column", "setLabelCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
