
package models.sparkworkflow.statement.machinelearning.classification

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * Output a logistic regression.
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#logistic-regression">
  */
final class LogisticRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "classification.LogisticRegression"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = LogisticRegression.ParamSetMethodMappings
}

object LogisticRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("elastic_net_param", "setElasticNetParam", "double"),
    ("fit_intercept", "setFitIntercept", "boolean"),
    ("max_iterations", "setMaxIter", "integer"),
    ("regularization_param", "setRegParam", "double"),
    ("tolerance", "setTol", "double"),
    ("standardization", "setStandardization", "boolean"),
    ("weight_column", "setWeightCol", "string"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("probability_column", "setProbabilityCol", "string"),
    ("raw_prediction_column", "setRawPredictionCol", "string"),
    ("prediction_column", "setPredictionCol", "string"),
    ("threshold", "setThreshold", "double")
  )
}
