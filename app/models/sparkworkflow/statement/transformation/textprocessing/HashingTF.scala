
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.textprocessing.HashingTF._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#tf-idf">
  */
final class HashingTF(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireInteger(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.HashingTF().setNumFeatures(${get(ExtraKeys.head)})"
}

object HashingTF {
  private val ExtraKeys: Vector[String] = Vector("num_features")
}
