
package models.sparkworkflow.statement.filtering

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.MultiColumnParamsInfo.MultiColumnParamKeys
import models.sparkworkflow.statement.filtering.HandleMissingValues._
import models.sparkworkflow.statement.MultiColumnParamsInfo

object HandleMissingValues {

  private val ExtraKeys : Vector[String] = Vector(
    "indicator_column_prefix",
    "strategy",
    "custom_value",
    "retain_empty_columns",
    "ud_missing_values",
    "delimiter_as_missing_value"
  )

  private val ParamKeys: Vector[String] = ExtraKeys ++ MultiColumnParamKeys

  private val SupportedStrategies = Set(
    "append_indicator",
    "remove_row",
    "remove_column",
    "replace_with_custom_value",
    "replace_with_mode",
    "replace_with_mean",
    "replace_with_median"
  )
}

/**
  * Handle Missing values with different strategies:
  *   1) Append indicator. Append associated `column` with boolean type if the column has missing values.
  *   2) Remove row. Remove the rows that contain missing values.
  *   3) Remove column. Remove the columns that contains missing values.
  *   4) Replace with custom value. Replace the missing values with custom value provided by user.
  *   5) Replace with custom value. Replace the missing values with column `mode` value provided by user.
  *   6) Replace with custom value. Replace the missing values with column `mean` value provided by user.
  *   7) Replace with custom value. Replace the missing values with column `median` value provided by user.
  */
final class HandleMissingValues(node: Node) extends Statement(node) with MultiColumnParamsInfo {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = ParamKeys

  private lazy val p = {
    val Vector(
      indicatorPrefix, strategy, customValue, retainEmptyColumns, udMissingValues,
      delimiterAsMissingValue, filterMode, colNames, colTypes, indexRanges
    ) = ParamKeys.map(get)

    Params(
      indicatorPrefix, strategy, customValue, retainEmptyColumns, udMissingValues,
      delimiterAsMissingValue, filterMode, colNames, colTypes, indexRanges
    )
  }

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val Vector(dataFrameOutput, transformerOutput) = selfOutputs

    s"""
       |import java.sql.Timestamp
       |import scala.util.Try
       |import org.apache.spark.sql.{DataFrame, Column, Row}
       |import org.apache.spark.sql.types._
       |import org.apache.spark.sql.functions.{col, when, desc, avg}
       |
       |def $scopingMethod(rawDF: DataFrame): DataFrame = {
       |
       |  $ExtractColumnsMethodDefinition
       |
       |  val selectedColumns: Array[String] = $ExtractColumnsOp(rawDF, "${p.filterMode}", "${p.colNames}", "${p.colTypes}", "${p.indexRanges}")
       |
       |  val missingValues: Array[String] = {
       |    val delimiter: Array[String] = if (${p.delimiterAsMissingValue}) Array(",") else Array()
       |    ("${p.udMissingValues}".split(",").map(_.trim) ++ delimiter).filterNot(_.isEmpty)
       |  }
       |
       |  def convertRawValue(field: StructField, rawValue: String): Any = {
       |    field.dataType match {
       |      case ByteType => rawValue.toByte
       |      case DecimalType() => new java.math.BigDecimal(rawValue)
       |      case DoubleType => rawValue.toDouble
       |      case FloatType => rawValue.toFloat
       |      case IntegerType => rawValue.toInt
       |      case LongType => rawValue.toLong
       |      case ShortType => rawValue.toShort
       |      case BooleanType => rawValue.toBoolean
       |      case StringType => rawValue
       |      case TimestampType => Timestamp.valueOf(rawValue)
       |    }
       |  }
       |
       |  val df = if ("${p.indicatorPrefix}".isEmpty) {
       |    rawDF
       |  } else {
       |    def hasMissingInColoumnInRawDF(colName: String): Column = {
       |      val isNullValue: Column = rawDF.schema(colName).dataType match {
       |        case _: DoubleType | FloatType => col(colName).isNull.or(col(colName).isNaN)
       |        case _ => col(colName).isNull
       |      }
       |      val field: StructField = rawDF.schema(colName)
       |      val convertedMissingValues: Array[Any] =
       |        missingValues.flatMap { colName => Try(convertRawValue(field, colName)).toOption }
       |      val hasInMissingValues: Column = col(colName).isin(convertedMissingValues: _*)
       |
       |      isNullValue.or(hasInMissingValues)
       |    }
       |
       |    (rawDF /: selectedColumns) { (dataFrame, colName) =>
       |      val newColName: String = "${p.indicatorPrefix}" + colName
       |      val newColumn: Column = hasMissingInColoumnInRawDF(colName).as(newColName).cast(BooleanType)
       |      dataFrame.withColumn(newColName, newColumn)
       |    }
       |  }
       |
       |  def removeRowsWithMissingValues(): DataFrame = {
       |    df.where(!hasMissingValuesInRow)
       |  }
       |
       |  def removeColumnsWithMissingValues(): DataFrame = {
       |    def columnHasMissingValues(colName: String): Boolean = {
       |      df.select(colName).where(hasMissingValuesInColumn(colName)).count > 0
       |    }
       |
       |    val columnsHaveMissingValues: Array[String] = selectedColumns.filter(columnHasMissingValues)
       |    val retainedColumns: Array[String] = df.columns.diff(columnsHaveMissingValues)
       |    df.select(retainedColumns.map(col): _*)
       |  }
       |
       |  def replaceWithCustomValue(): DataFrame = {
       |    val targetColumnTypes: Set[String] = selectedColumns.map(df.schema(_).dataType.typeName).toSet
       |    if (targetColumnTypes.size != 1) {
       |      val error =
       |        s"Cannot convert custom value `${p.customValue}` into multi types [$${targetColumnTypes.mkString(", ")}]"
       |      throw new Exception(error)
       |    }
       |
       |    val newCols: Array[Column] = df.columns.map { colName =>
       |      if (selectedColumns.contains(colName)) {
       |        val field: StructField = df.schema(colName)
       |        val newValue = convertRawValue(field, "${p.customValue}")
       |        when(hasMissingValuesInColumn(colName), newValue).otherwise(col(colName)).as(colName)
       |      } else {
       |        col(colName)
       |      }
       |    }
       |
       |    df.select(newCols: _*)
       |  }
       |
       |  def replaceWith(columnReplacements: Array[(String, Option[Any])]): DataFrame =  {
       |
       |    val colReplacementMap: Map[String, Any] =
       |      columnReplacements.collect { case (colName, Some(value)) => (colName, value) }.toMap
       |
       |    val replacedCols: Array[Column] = df.columns.map { colName =>
       |      colReplacementMap.get(colName) match {
       |        case Some(replacement) =>
       |          when(hasMissingValuesInColumn(colName), replacement).otherwise(col(colName)).as(colName)
       |        case None => col(colName)
       |      }
       |    }
       |
       |    val replacedDF = df.select(replacedCols: _*)
       |    if (${p.retainEmptyColumns}) {
       |      replacedDF
       |    } else {
       |      val notAllEmptyColumns: Array[String] = colReplacementMap.keys.toArray
       |      val allEmptyColumns: Array[String] = selectedColumns.diff(notAllEmptyColumns)
       |      val restCols: Array[String] = df.columns.diff(allEmptyColumns)
       |      replacedDF.select(restCols.map(col): _*)
       |    }
       |  }
       |
       |  def replaceWithMode(): DataFrame = {
       |    def calculateMode(colName: String): Option[Any] = {
       |      val modeWithCount: Array[Row] =
       |        df
       |          .select(colName)
       |          .where(!hasMissingValuesInColumn(colName))
       |          .groupBy(col(colName))
       |          .count()
       |          .orderBy(desc("count"))
       |          .limit(1)
       |          .collect()
       |      if (modeWithCount.isEmpty) None else Some(modeWithCount(0)(0))
       |    }
       |
       |    val columnModes: Array[(String, Option[Any])] = selectedColumns.map(col => (col, calculateMode(col)))
       |
       |    replaceWith(columnModes)
       |  }
       |
       |  def replaceWithMean(): DataFrame = {
       |    def calculateMean(colName: String): Option[Any] = {
       |      val meanRows: Array[Row] =
       |        df
       |          .where(!hasMissingValuesInColumn(colName))
       |          .select(avg(colName))
       |          .collect()
       |      val mean = meanRows(0)(0)
       |      if (mean == null) None else Some(mean)
       |    }
       |
       |    val columnMeans: Array[(String, Option[Any])] = selectedColumns.map(col => (col, calculateMean(col)))
       |
       |    replaceWith(columnMeans)
       |  }
       |
       |  def replaceWithMedian(): DataFrame = {
       |    def calculateMedian(colName: String): Option[Any] = {
       |      // If column are all missing values, this would throw exception
       |      // Can be verified by:
       |      //  val newDF = df.withColumn("null_cols", lit(null).cast(DoubleType))
       |      try {
       |        val medians: Array[Double] =
       |          df
       |            .where(!hasMissingValuesInColumn(colName))
       |            .stat
       |            .approxQuantile(colName, Array(0.5), 0.001)
       |        Some(medians(0))
       |      } catch { case ex: java.util.NoSuchElementException => None }
       |    }
       |
       |    val columnMedians: Array[(String, Option[Any])] = selectedColumns.map(col => (col, calculateMedian(col)))
       |
       |    replaceWith(columnMedians)
       |  }
       |
       |  def hasMissingValuesInRow(): Column = {
       |    val predicates: Array[Column] = selectedColumns.map(hasMissingValuesInColumn)
       |    predicates.reduce(_ or _)
       |  }
       |
       |  def hasMissingValuesInColumn(colName: String): Column = {
       |
       |    val isNullValue = df.schema(colName).dataType match {
       |      case _: DoubleType | FloatType => col(colName).isNull.or(col(colName).isNaN)
       |      case _ => col(colName).isNull
       |    }
       |
       |    val convertedMissingValues = convertMissingValuesToColumnType(colName)
       |    val hasInMissingValues = col(colName).isin(convertedMissingValues: _*)
       |
       |    isNullValue.or(hasInMissingValues)
       |  }
       |
       |  def convertMissingValuesToColumnType(colName: String): Array[Any] = {
       |    val field: StructField = df.schema(colName)
       |    missingValues.flatMap { colName => Try(convertRawValue(field, colName)).toOption }
       |  }
       |
       |  val strategy = "${p.strategy}"
       |  strategy match {
       |    case "append_indicator" => df
       |    case "remove_row" => removeRowsWithMissingValues()
       |    case "remove_column" => removeColumnsWithMissingValues()
       |    case "replace_with_custom_value" => replaceWithCustomValue()
       |    case "replace_with_mode" => replaceWithMode()
       |    case "replace_with_mean" => replaceWithMean()
       |    case "replace_with_median" => replaceWithMedian()
       |  }
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }

  case class Params(
      indicatorPrefix: String,
      strategy: String,
      customValue: String,
      retainEmptyColumns: String,
      udMissingValues: String,
      delimiterAsMissingValue: String,
      filterMode: String,
      colNames: String,
      colTypes: String,
      indexRanges: String)
    extends MultiColumnParams(filterMode, colNames, colTypes, indexRanges) {

    require(
      SupportedStrategies.contains(strategy),
      s"$concreteStatement: Expect strategy to be one of ${SupportedStrategies.mkString(", ")}, got: `$strategy`"
    )

    require(
      !invalidCustomValue_?,
      s"$concreteStatement: custom value CANNOT be empty when strategy is `replace_with_custom_value`"
    )

    require(
      delimiterAsMissingValue == "true" || delimiterAsMissingValue == "false",
      s"$concreteStatement: delimiter_as_missing_value must be string of 'true' or 'false'"
    )

    require(
      validRetainEmptyColumns_?,
      s"$concreteStatement retain_empty_columns must be string of 'true' or 'false' when strategy is `replace_with_mode`"
    )

    require(
      !missIndicatorPrefix_?,
      s"$concreteStatement: indicator_column_prefix cannot be empty when strategy is `append_indicator`"
    )

    private def validRetainEmptyColumns_? : Boolean = {
      if (strategy != "replace_with_mode") {
        true
      } else {
        retainEmptyColumns == "true" || retainEmptyColumns == "false"
      }
    }

    private def invalidCustomValue_? : Boolean = {
      strategy == "replace_with_custom_value" && customValue.isEmpty
    }

    private def missIndicatorPrefix_? : Boolean = {
      strategy == "append_indicator" && indicatorPrefix.isEmpty
    }
  }
}

