
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.transformation.featureconversion.ComposeDatetime._

object ComposeDatetime {

  private val SupportedIndicators = Set("name", "index")

  private val SupportedParts = Set("year", "month", "day", "hour", "minute", "second")

  /**
    * From year to second, they all share same format as indicator:value
    * e.g.
    *   {
    *     "year": "name:fiscal_year",
    *     "day": "index: 7"
    *   }
    */
  private val ParamKeys = Vector("year", "month", "day", "hour", "minute", "second", "output_column")
}

/**
  * Compose an new column with Timestamp type by the given date data from existing columns.
  */
final class ComposeDatetime(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = ParamKeys

  private def partIsValid(part: String): Unit = {
    val Array(indicator, value) = get(part).split(":")
    require(
      SupportedParts.contains(part) && SupportedIndicators.contains(indicator),
      s"$concreteStatement: $part format is invalid"
    )
  }

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)

    val nonEmptyKeys: Vector[String] = ParamKeys.take(6).filterNot(get(_).isEmpty)

    nonEmptyKeys.foreach(partIsValid)

    // concat as "year:name:fiscal_year,month:index:9,day:name:fiscal_day"
    val timestampParams =
      nonEmptyKeys
        .map(key => s"$key:${get(key).trim}") // map as "partName:indicator:value", e.g. "year:name:fiscal_year"
        .mkString(",")  // concat as string with ','

    val outputColumn = get(ParamKeys.last)

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.sql.types.{DoubleType, TimestampType}
       |import org.apache.spark.sql.functions.{concat_ws, format_string, lit, unix_timestamp}
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |  case class TimestampPart(partName: String, defaultValue: Int, formatPattern: String)
       |  val timestampParts: Array[TimestampPart] = Array(
       |    TimestampPart("year", 1970, "%04.0f"),
       |    TimestampPart("month", 1, "%02.0f"),
       |    TimestampPart("day", 1, "%02.0f"),
       |    TimestampPart("hour", 0, "%02.0f"),
       |    TimestampPart("minute", 0, "%02.0f"),
       |    TimestampPart("second", 0, "%02.0f")
       |  )
       |
       |  val timestampColumns: Map[String, String] =
       |    "$timestampParams".split(",").map { param =>
       |      val Array(part, indicator, value) = param.split(":")
       |      val colName = if (indicator == "name") value else df.schema.fieldNames(value.toInt)
       |      (part, colName)
       |    }.toMap
       |
       |  val partColumns = timestampParts.map { part =>
       |    timestampColumns.get(part.partName) match {
       |      case Some(column) => format_string(part.formatPattern, col(column).cast(DoubleType))
       |      case None => lit(part.formatPattern.format(part.defaultValue.toDouble))
       |    }
       |  }
       |
       |  // concat as "yyyy-mm-dd hh:mm:ss"
       |  val newColumn = unix_timestamp(
       |    concat_ws(" ",
       |      concat_ws("-", partColumns(0), partColumns(1), partColumns(2)),
       |      concat_ws(":", partColumns(3), partColumns(4), partColumns(5))
       |    )
       |  ).cast(TimestampType)
       |
       |  df.withColumn("$outputColumn", newColumn)
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

