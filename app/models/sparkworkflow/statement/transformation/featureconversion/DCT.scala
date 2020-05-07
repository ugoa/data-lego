
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.DCT._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#discrete-cosine-transform-dct">
  */
final class DCT(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireBoolean(ExtraKeys)

  override lazy val newStageMethodBody: String = {
    s"new org.apache.spark.ml.feature.DCT().setInverse(${get(ExtraKeys.head)})"
  }
}

object DCT {

  private val ExtraKeys = Vector("inverse")
}
