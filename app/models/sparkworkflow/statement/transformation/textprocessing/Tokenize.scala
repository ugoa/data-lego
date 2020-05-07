
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#tokenizer">
  */
final class Tokenize(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = Vector()

  override lazy val newStageMethodBody: String = s"new org.apache.spark.ml.feature.Tokenizer()"
}
