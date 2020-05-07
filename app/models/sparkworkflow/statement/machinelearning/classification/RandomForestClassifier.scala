
package models.sparkworkflow.statement.machinelearning.classification

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * Output an Random Forest classifier
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#random-forest-classifier">
  */
final class RandomForestClassifier(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "classification.RandomForestClassifier"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = RandomForestClassifier.ParamSetMethodMappings
}

object RandomForestClassifier {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("max_depth", "setMaxDepth", "integer"),
    ("max_bins", "setMaxBins", "integer"),
    ("min_instances_per_node", "setMinInstancesPerNode", "integer"),
    ("min_information_gain", "setMinInfoGain", "double"),
    ("max_memory", "setMaxMemoryInMB", "integer"),
    ("cache_node_ids", "setCacheNodeIds", "boolean"),
    ("checkpoint_interval", "setCheckpointInterval", "integer"),
    ("classification_impurity", "setImpurity", "string"),
    ("subsampling_rate", "setSubsamplingRate", "double"),
    ("seed", "setSeed", "long"),
    ("feature_subset_strategy", "setFeatureSubsetStrategy", "string"),
    ("num_trees", "setNumTrees", "integer"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("probability_column", "setProbabilityCol", "string"),
    ("raw_prediction_column", "setRawPredictionCol", "string"),
    ("prediction_column", "setPredictionCol", "string")
  )
}
