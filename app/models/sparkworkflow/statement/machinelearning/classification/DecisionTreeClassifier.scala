
package models.sparkworkflow.statement.machinelearning.classification

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * Output a Decision Tree Classifier
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#decision-tree-classifier">
  */
final class DecisionTreeClassifier(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "classification.DecisionTreeClassifier"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = DecisionTreeClassifier.ParamSetMethodMappings
}

object DecisionTreeClassifier {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("cache_node_ids", "setCacheNodeIds", "boolean"),
    ("checkpoint_interval", "setCheckpointInterval", "integer"),
    ("regression_impurity", "setImpurity", "string"),
    ("max_depth", "setMaxDepth", "integer"),
    ("max_bins", "setMaxBins", "integer"),
    ("max_memory", "setMaxMemoryInMB", "integer"),
    ("min_instances_per_node", "setMinInstancesPerNode", "integer"),
    ("min_information_gain", "setMinInfoGain", "double"),
    ("seed", "setSeed", "long"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("probability_column", "setProbabilityCol", "string"),
    ("raw_prediction_column", "setRawPredictionCol", "string"),
    ("prediction_column", "setPredictionCol", "string"),
    ("thresholds", "setThresholds", "double_array")
  )
}
