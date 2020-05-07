
package models.sparkworkflow.statement.machinelearning.classification

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * Output an Naive Bayes classifier
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#naive-bayes">
  */
final class NaiveBayes(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "classification.NaiveBayes"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = NaiveBayes.ParamSetMethodMappings
}

object NaiveBayes {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("model_type", "setModelType", "string"),
    ("smoothing", "setSmoothing", "double"),
    ("label_column", "setLabelCol", "string"),
    ("weight_column", "setWeightCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("probability_column", "setProbabilityCol", "string"),
    ("raw_prediction_column", "setRawPredictionCol", "string"),
    ("prediction_column", "setPredictionCol", "string"),
    ("thresholds", "setThresholds", "double_array")
  )
}
