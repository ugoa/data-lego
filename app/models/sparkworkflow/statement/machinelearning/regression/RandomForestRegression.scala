
package models.sparkworkflow.statement.machinelearning.regression

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression">
  */
final class RandomForestRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "regression.RandomForestRegressor"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = RandomForestRegression.ParamSetMethodMappings
}

object RandomForestRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("max_depth", "setMaxDepth", "integer"),
    ("max_bins", "setMaxBins", "integer"),
    ("min_instances_per_node", "setMinInstancesPerNode", "integer"),
    ("min_information_gain", "setMinInfoGain", "double"),
    ("max_memory", "setMaxMemoryInMB", "integer"),
    ("cache_node_ids", "setCacheNodeIds", "boolean"),
    ("checkpoint_interval", "setCheckpointInterval", "integer"),
    ("regression_impurity", "setImpurity", "string"),
    ("subsampling_rate", "setSubsamplingRate", "double"),
    ("seed", "setSeed", "long"),
    ("num_trees", "setNumTrees", "integer"),
    ("feature_subset_strategy", "setFeatureSubsetStrategy", "string"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
