
package models.sparkworkflow.statement.machinelearning.dimensionalityreduction

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.InputOutputColumnStatement.EmptyInputOutputParamMap
import models.sparkworkflow.statement.machinelearning.dimensionalityreduction.PCA._

final class PCA(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  override lazy val formattedParameters: Map[String, String] = {
    val originMap = OriginKeys.map(key => key -> getRaw(key)).toMap
    EmptyInputOutputParamMap ++ Map("operate_on" -> "one") ++ originMap
  }

  override lazy val newStageMethodBody: String =
    s"new org.apache.spark.ml.feature.PCA().setK(${get(ExtraKeys.head)})"
}

object PCA {
  private val ExtraKeys: Vector[String] = Vector("k")

  private val OriginKeys: Vector[String] =
    Vector("input_column", "input_column_index", "output_mode", "append_column") ++ ExtraKeys
}
