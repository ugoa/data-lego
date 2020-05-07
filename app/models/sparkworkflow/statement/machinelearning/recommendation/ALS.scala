
package models.sparkworkflow.statement.machinelearning.recommendation

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.machinelearning.MachineLearningStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-collaborative-filtering.html#collaborative-filtering">
  */
final class ALS(node: Node) extends MachineLearningStatement(node) {

  override lazy val subPackage: String = "recommendation.ALS"

  override lazy val paramSetMethodMappings: Vector[(String, String, String)] = ALS.ParamSetMethodMappings
}

object ALS {
  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("alpha", "setAlpha", "double"),
    ("checkpoint_interval", "setCheckpointInterval", "integer"),
    ("final_storage_level", "setFinalStorageLevel", "string"),
    ("implicit_prefs", "setImplicitPrefs", "boolean"),
    ("intermediate_storage_level", "setIntermediateStorageLevel", "string"),
    ("item_column", "setItemCol", "string"),
    ("max_iterations", "setMaxIter", "integer"),
    ("nonnegative", "setNonnegative", "boolean"),
    ("num_blocks", "setNumBlocks", "integer"),
    ("num_item_blocks", "setNumItemBlocks", "integer"),
    ("num_user_blocks", "setNumUserBlocks", "integer"),
    ("prediction_column", "setPredictionCol", "string"),
    ("rank", "setRank", "integer"),
    ("rating_column", "setRatingCol", "string"),
    ("reg_param", "setRegParam", "double"),
    ("seed", "setSeed", "long"),
    ("user_column", "setUserCol", "string")
  )
}
