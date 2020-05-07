
package models.sparkworkflow.statement.machinelearning.modelevaluation

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.Evaluator
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator">
  */
final class MulticlassClassificationEvaluator(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "evaluation.MulticlassClassificationEvaluator"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = MulticlassClassificationEvaluator.ParamSetMethodMappings

  override lazy val selfOutputTypes: Vector[String] = Vector(Evaluator)
}

object MulticlassClassificationEvaluator {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("label_column", "setLabelCol", "string"),
    ("prediction_column", "setPredictionCol", "string"),
    ("metric_name", "setMetricName", "string")
  )
}
