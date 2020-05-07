
package models.sparkworkflow.statement.machinelearning.regression

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression">
  */
final class GBTRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = GBTRegression.ParamSetMethodMappings

  override lazy val subPackage: String = "regression.GBTRegressor"
}

object GBTRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("loss_function", "setLossType", "string"),
    ("max_bins", "setMaxBins", "integer"),
    ("max_depth", "setMaxDepth", "integer"),
    ("max_iterations", "setMaxIter", "integer"),
    ("max_memory", "setMaxMemoryInMB", "integer"),
    ("min_information_gain", "setMinInfoGain", "double"),
    ("min_instances_per_node", "setMinInstancesPerNode", "integer"),
    ("seed", "setSeed", "long"),
    ("step_size", "setStepSize", "double"),
    ("subsampling_rate", "setSubsamplingRate", "double"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
