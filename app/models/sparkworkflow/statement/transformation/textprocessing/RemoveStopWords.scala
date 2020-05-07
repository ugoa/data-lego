
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.textprocessing.RemoveStopWords._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#stopwordsremover">
  */
final class RemoveStopWords(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  requireBoolean(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.StopWordsRemover().setCaseSensitive(${get(ExtraKeys.head)})"
}

object RemoveStopWords {
  private val ExtraKeys = Vector("case_sensitive")
}
