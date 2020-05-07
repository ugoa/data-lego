
package models.sparkworkflow.statement.machinelearning.recommendation

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-frequent-pattern-mining.html#fp-growth">
  */
final class FPGrowth(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "fpm.FPGrowth"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = FPGrowth.ParamSetMethodMappings
}

object FPGrowth {
  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("items_column", "setItemsCol", "string"),
    ("min_confidence", "setMinConfidence", "double"),
    ("min_support", "setMinSupport", "double"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
