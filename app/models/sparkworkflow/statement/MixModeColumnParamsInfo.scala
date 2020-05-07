
package models.sparkworkflow.statement

import models.sparkworkflow.statement.MultiColumnParamsInfo.MultiColumnParamKeys
import models.sparkworkflow.statement.MixModeColumnParamsInfo._

object MixModeColumnParamsInfo {

  val MixModeColumnParamKeys: Vector[String] = MultiColumnParamKeys ++ Vector(
    "operate_on",
    "input_column",
    "input_column_index",
    "output_mode",
    "append_column",
    "append_columns_prefix"
  )

  val SupportedColTypes = Set("numeric", "string", "timestamp", "boolean")

  val OperationModes = Set("one", "multiple")

  val OutputModes = Set("replace", "append")

  val DefaultPrefix = "appended__"
}

/**
  * Trait that extract and validate params of the statement which take one or more columns as input.
  */
trait MixModeColumnParamsInfo extends MultiColumnParamsInfo {

  def getColParam(key: String): String

  def getMixModeColumnParams: MixModeColumnParams = {

    val operateOn = getColParam("operate_on").toLowerCase
    val outputMode = getColParam("output_mode").toLowerCase

    val appendCol = getColParam("append_column")
    val colPrefix = {
      val v = if (operateOn == "multiple") getColParam("append_columns_prefix") else DefaultPrefix
      if (v.endsWith("_")) v else v + "_"
    }

    val filterMode = if (operateOn == "one") "including" else getColParam("filter_mode")
    val colNames =
      if (operateOn == "one") getColParam("input_column") else getColParam("filter_column_names")
    val colTypes = getColParam("filter_column_types")
    val indexRanges = {
      if (operateOn == "one") {
        val index = getColParam("input_column_index")
        if (index.isEmpty) "" else s"$index-$index"
      } else getColParam("filter_column_index_ranges")
    }

    MixModeColumnParams(
      operateOn, outputMode, appendCol, colPrefix, filterMode, colNames, colTypes, indexRanges
    )
  }

  case class MixModeColumnParams(
      operateOn: String,
      outputMode: String,
      appendCol: String,
      colPrefix: String,
      filterMode: String,
      colNames: String,
      colTypes: String,
      indexRanges: String) extends MultiColumnParams(filterMode, colNames, colTypes, indexRanges) {

    require(
      !invalidAppendCol_?,
      s"$statement: Append column CANNOT be empty when operate_on is 'one' and output_mode is 'append'"
    )

    require(
      !invalidColPrefix_?,
      s"$statement: column prefix CANNOT be empty when operate_on is 'multiple' and output_mode is 'append'"
    )

    require(
      OperationModes.contains(operateOn),
      s"$statement: operate_on must be neither 'one' or 'multiple'"
    )

    require(
      OutputModes.contains(outputMode),
      s"$statement output_mode must be neither 'replace'or 'append'"
    )

    private def invalidAppendCol_? : Boolean = {
      operateOn == "one" && outputMode == "append" && appendCol.isEmpty
    }

    private def invalidColPrefix_? : Boolean = {
      operateOn == "multiple" && outputMode == "append" && colPrefix.isEmpty
    }
  }
}
