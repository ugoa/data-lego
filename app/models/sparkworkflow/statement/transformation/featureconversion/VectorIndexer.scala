
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.InputOutputColumnStatement.EmptyInputOutputParamMap
import models.sparkworkflow.statement.transformation.featureconversion.VectorIndexer._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#vectorindexer">
  */
final class VectorIndexer(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  /**
    * Parameters of VectorIndexer will only need to be VectorIndexer.OriginKeys
    * but in order to use InputOutputColumnStatement, we add extra keys to fuifill the requirement.
    */
  override lazy val formattedParameters: Map[String, String] = {
    val originMap = OriginKeys.map(key => key -> getRaw(key)).toMap
    EmptyInputOutputParamMap ++ Map("operate_on" -> "one") ++ originMap
  }

  requireInteger(ExtraKeys)

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.VectorIndexer().setMaxCategories(${get(ExtraKeys.head)})"
}

object VectorIndexer {

  private val ExtraKeys: Vector[String] = Vector("max_categories")

  private val OriginKeys: Vector[String] =
    Vector("input_column", "input_column_index", "output_mode", "append_column") ++ ExtraKeys
}
