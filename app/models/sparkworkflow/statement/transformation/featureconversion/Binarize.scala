
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.Binarize._

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-features.html#binarizer">
  */
final class Binarize(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireNonEmpty(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.Binarizer().setThreshold(${get(ExtraKeys.head)})"
}

object Binarize {
  private val ExtraKeys = Vector("threshold")
}
