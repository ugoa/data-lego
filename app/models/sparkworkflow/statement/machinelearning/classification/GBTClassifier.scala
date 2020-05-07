
package models.sparkworkflow.statement.machinelearning.classification

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * Output a Gradient-boosted tree classifier
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#gradient-boosted-tree-classifier">
  */
final class GBTClassifier(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "classification.GBTClassifier"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = GBTClassifier.ParamSetMethodMappings
}

object GBTClassifier {

  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("cache_node_ids", "setCacheNodeIds", "boolean"),
    ("checkpoint_interval", "setCheckpointInterval", "integer"),
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