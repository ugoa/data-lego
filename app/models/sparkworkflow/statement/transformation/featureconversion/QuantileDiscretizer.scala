
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.QuantileDiscretizer._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#quantilediscretizer">
  */
final class QuantileDiscretizer(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireInteger(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.QuantileDiscretizer().setNumBuckets(${get(ExtraKeys.head)})"
}

object QuantileDiscretizer {
  private val ExtraKeys: Vector[String] = Vector("num_buckets")
}
