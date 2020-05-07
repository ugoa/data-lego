
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.PolynomialExpansion._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#polynomialexpansion">
  */
final class PolynomialExpansion(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireInteger(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.PolynomialExpansion().setDegree(${get(ExtraKeys.head)})"
}

object PolynomialExpansion {
  private val ExtraKeys = Vector("degree")
}
