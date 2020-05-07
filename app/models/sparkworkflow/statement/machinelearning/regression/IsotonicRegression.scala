
package models.sparkworkflow.statement.machinelearning.regression

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression">
  */
final class IsotonicRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = IsotonicRegression.ParamSetMethodMappings

  override lazy val subPackage: String = "regression.IsotonicRegression"
}

object IsotonicRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("isotonic", "setIsotonic", "boolean"),
    ("feature_index", "setFeatureIndex", "integer"),
    ("weight_column", "setWeightCol", "string"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
