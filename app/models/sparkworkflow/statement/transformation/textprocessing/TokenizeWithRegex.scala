
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.textprocessing.TokenizeWithRegex._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#tokenizer">
  */
final class TokenizeWithRegex(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireBoolean(Vector("gaps"))

  requireInteger(Vector("min_token_length"))

  override lazy val newStageMethodBody: String = {
    val Vector(gaps, minTokenLength, pattern) = ExtraKeys.map(get)
    s"""new org.apache.spark.ml.feature.RegexTokenizer().setGaps($gaps).setMinTokenLength($minTokenLength).setPattern("$pattern")"""
  }
}

object TokenizeWithRegex {
  private val ExtraKeys = Vector("gaps", "min_token_length", "pattern")
}
