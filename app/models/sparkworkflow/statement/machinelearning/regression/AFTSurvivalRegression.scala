
package models.sparkworkflow.statement.machinelearning.regression

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#survival-regression">
  */
final class AFTSurvivalRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "regression.AFTSurvivalRegression"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = AFTSurvivalRegression.ParamSetMethodMappings
}

object AFTSurvivalRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("fit_intercept", "setFitIntercept", "boolean"),
    ("max_iterations", "setMaxIter", "integer"),
    ("tolerance", "setTol", "double"),
    ("label_column", "setLabelCol", "string"),
    ("censor_column", "setCensorCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string"),
    ("quantile_probabilities", "setQuantileProbabilities", "double_array"),
    ("quantiles_column", "setQuantilesCol", "string")
  )
}
