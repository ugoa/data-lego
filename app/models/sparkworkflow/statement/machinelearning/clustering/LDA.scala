
package models.sparkworkflow.statement.machinelearning.clustering

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-features.html#pca">
  */
final class LDA(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "clustering.LDA"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = LDA.ParamSetMethodMappings
}

object LDA {
  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("k", "setK", "integer"),
    ("optimizer", "setOptimizer", "string"),
    ("topic_distribution_column", "setTopicDistributionCol", "string"),
    ("doc_concentration", "setDocConcentration", "double_array"),
    ("learning_offset", "setLearningOffset", "double"),
    ("learning_decay", "setLearningDecay", "double"),
    ("optimize_doc_concentration", "setOptimizeDocConcentration", "boolean"),
    ("keep_last_checkpoint", "setKeepLastCheckpoint", "boolean"),
    ("max_iterations", "setMaxIter", "double"),
    ("seed", "setSeed", "long"),
    ("subsampling_rate", "setSubsamplingRate", "double"),
    ("features_column", "setFeaturesCol", "string")
  )
}
