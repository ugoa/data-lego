
package models.sparkworkflow.statement.machinelearning.regression

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression">
  */
final class LinearRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "regression.LinearRegression"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = LinearRegression.ParamSetMethodMappings
}

object LinearRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("elastic_net_param", "setElasticNetParam", "double"),
    ("fit_intercept", "setFitIntercept", "boolean"),
    ("max_iterations", "setMaxIter", "double"),
    ("regularization_param", "setRegParam", "double"),
    ("tolerance", "setTol", "double"),
    ("standardization", "setStandardization", "boolean"),
    ("solver", "setSolver", "string"),
    ("weight_column", "setWeightCol", "string"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
