
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.InputOutputColumnStatement.EmptyInputOutputParamMap
import models.sparkworkflow.statement.transformation.textprocessing.IDF._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#tf-idf">
  */
final class IDF(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  override lazy val formattedParameters: Map[String, String] = {
    val originMap = OriginKeys.map(key => key -> getRaw(key)).toMap
    EmptyInputOutputParamMap ++ Map("operate_on" -> "one") ++ originMap
  }

  requireInteger(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.IDF().setMinDocFreq(${get(ExtraKeys.head)})"
}

object IDF {
  private val ExtraKeys: Vector[String] = Vector("min_documents_frequency")

  private val OriginKeys: Vector[String] =
    Vector("input_column", "input_column_index", "output_mode", "append_column") ++ ExtraKeys
}
