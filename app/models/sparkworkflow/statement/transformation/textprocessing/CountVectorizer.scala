
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.InputOutputColumnStatement.EmptyInputOutputParamMap
import models.sparkworkflow.statement.transformation.textprocessing.CountVectorizer._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#countvectorizer">
  */
final class CountVectorizer(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  override lazy val formattedParameters: Map[String, String] = {
    val originMap = OriginKeys.map(key => key -> getRaw(key)).toMap
    EmptyInputOutputParamMap ++ Map("operate_on" -> "one") ++ originMap
  }

  requireInteger(ExtraKeys.take(1))

  requireNumeric(ExtraKeys.takeRight(2))

  override lazy val newStageMethodBody: String = {
    val Vector(vocabSize, minDF, minTF) = ExtraKeys.map(get)
    s"new org.apache.spark.ml.feature.CountVectorizer().setVocabSize($vocabSize).setMinDF($minDF).setMinTF($minTF)"
  }
}

object CountVectorizer {

  private val ExtraKeys = Vector("max_vocabulary_size", "min_different_documents", "min_term_frequency")

  private val OriginKeys: Vector[String] =
    Vector("input_column", "input_column_index", "output_mode", "append_column") ++ ExtraKeys
}
