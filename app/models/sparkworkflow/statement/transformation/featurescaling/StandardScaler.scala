
package models.sparkworkflow.statement.transformation.featurescaling

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.InputOutputColumnStatement.EmptyInputOutputParamMap
import models.sparkworkflow.statement.transformation.featurescaling.StandardScaler._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#standardscaler">
  */
final class StandardScaler(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  /**
    * Parameters of VectorIndexer will only need to be VectorIndexer.OriginKeys
    * but in order to use InputOutputColumnStatement, we add extra keys to fuifill the requirement.
    */
  override lazy val formattedParameters: Map[String, String] = {
    val originMap = OriginKeys.map(key => key -> getRaw(key)).toMap
    EmptyInputOutputParamMap ++ Map("operate_on" -> "one") ++ originMap
  }

  requireBoolean(ExtraKeys)

  override lazy val newStageMethodBody: String = {
    val Vector(withMean, withStd) = ExtraKeys.map(get(_).toLowerCase)
    s"new org.apache.spark.ml.feature.StandardScaler().setWithMean($withMean).setWithStd($withStd)"
  }
}

object StandardScaler {
  private val ExtraKeys: Vector[String] = Vector("with_mean", "with_std")

  private val OriginKeys: Vector[String] =
    Vector("input_column", "input_column_index", "output_mode", "append_column") ++ ExtraKeys
}
