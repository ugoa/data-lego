
package models.sparkworkflow.statement.machinelearning.clustering

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-clustering.html#k-means">
  */
final class KMeans(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "clustering.KMeans"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = KMeans.ParamSetMethodMappings
}

object KMeans {
  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("k", "setK", "integer"),
    ("init_mode", "setInitMode", "string"),
    ("init_steps", "setInitSteps", "integer"),
    ("max_iterations", "setMaxIter", "double"),
    ("seed", "setSeed", "long"),
    ("tolerance", "setTol", "double"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
