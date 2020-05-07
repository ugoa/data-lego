
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.OneHotEncoder._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#onehotencoder">
  */
final class OneHotEncoder(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireBoolean(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.OneHotEncoder().setDropLast(${get(ExtraKeys.head)})"
}

object OneHotEncoder {

  private val ExtraKeys = Vector("drop_last")
}
