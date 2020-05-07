
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.textprocessing.ConvertToNGrams._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#n-gram">
  */
final class ConvertToNGrams(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireInteger(ExtraKeys)

  override lazy val newStageMethodBody: String = s"new org.apache.spark.ml.feature.NGram().setN(${get(ExtraKeys.head)})"
}

object ConvertToNGrams {
  private val ExtraKeys = Vector("n")
}
