
package models.sparkworkflow.statement.machinelearning.classification

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * Output a Multilayer perceptron classifier (MLPC)
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#multilayer-perceptron-classifier">
  */
final class MultilayerPerceptionClassifier(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "classification.MultilayerPerceptronClassifier"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = MultilayerPerceptionClassifier.ParamSetMethodMappings
}

object MultilayerPerceptionClassifier {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("seed", "setSeed", "long"),
    ("block_size", "setBlockSize", "integer"),
    ("layers", "setLayers", "integer_array"),
    ("max_iterations", "setMaxIter", "integer"),
    ("tolerance", "setTol", "double"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
