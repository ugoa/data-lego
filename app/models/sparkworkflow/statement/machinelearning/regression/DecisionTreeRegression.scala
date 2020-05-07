
package models.sparkworkflow.statement.machinelearning.regression

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-regression">
  */
final class DecisionTreeRegression(node: Node) extends MachineLearningStatement(node) {

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = DecisionTreeRegression.ParamSetMethodMappings

  override lazy val subPackage: String = "regression.DecisionTreeRegressor"
}

object DecisionTreeRegression {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("max_depth", "setMaxDepth", "integer"),
    ("max_bins", "setMaxBins", "integer"),
    ("min_instances_per_node", "setMinInstancesPerNode", "integer"),
    ("min_information_gain", "setMinInfoGain", "double"),
    ("max_memory", "setMaxMemoryInMB", "integer"),
    ("cache_node_ids", "setCacheNodeIds", "boolean"),
    ("regression_impurity", "setImpurity", "string"),
    ("checkpoint_interval", "setCheckpointInterval", "integer"),
    ("seed", "setSeed", "long"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
