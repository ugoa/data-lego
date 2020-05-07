
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.Normalize._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#normalizer">
  */
final class Normalize(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireNumeric(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.Normalizer().setP(${get(ExtraKeys.head)}.toDouble)"
}

object Normalize {
  private val ExtraKeys = Vector("p")
}
