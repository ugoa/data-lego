
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.transformation.featureconversion.DecomposeDatetime._

/**
  * Take a Timestamp column and decompose the date time into 6 extra columns: year/month/day/hour/minute/second.
  */
final class DecomposeDatetime(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = ParamKeys

  private lazy val Vector(parts, timestampColumn, rawPrefix) = ParamKeys.map(get)

  private lazy val prefix = {
    val p = if (rawPrefix.isEmpty) timestampColumn else rawPrefix
    if (p.endsWith("_")) p else p + "_"
  }

  private def requirePartsValid_? : Boolean = {
    val partList = parts.split(",").map(_.trim.toLowerCase)
    partList.forall(SupportedParts)
  }

  requireNonEmpty(ParamKeys.take(2))

  require(
    requirePartsValid_?,
    s"$concreteStatement: Expected parts to be subset of ${SupportedParts.mkString(", ")}, got: $parts"
  )

  require(
    get("timestamp_column").startsWith("name:") || get("timestamp_column").startsWith("index:"),
    s"$concreteStatement: Expect input of timestamp column to be in format of 'name:updated_at' or 'index:9'"
  )

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)

    s"""
       |import org.apache.spark.sql.{DataFrame, functions, Column}
       |import org.apache.spark.sql.functions.col
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |
       |  val partFuncMappings: Map[String, Column => Column] = Map(
       |    "year" -> functions.year _,
       |    "month" -> functions.month _,
       |    "day" -> functions.dayofmonth _,
       |    "hour" -> functions.hour _,
       |    "minute" -> functions.minute _,
       |    "second" -> functions.second _
       |  )
       |
       |  val rawColumn = "$timestampColumn"
       |  val Array(indicator, value) = rawColumn.split(",")
       |  val timestampCol: String =
       |    if (indicator == "index") df.schema.fieldNames(value.toInt) else value
       |
       |  val datetimeParts: Array[String] = "$parts".split(",").map(_.trim.toLowerCase)
       |
       |  (df /: datetimeParts) { (dataFrame, part) =>
       |    val newColName = "$prefix" + part
       |    val func = partFuncMappings(part)
       |    dataFrame.withColumn(newColName, func(col(timestampCol)))
       |  }
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}


object DecomposeDatetime {

  private val ParamKeys = Vector("parts", "timestamp_column", "prefix")

  private val SupportedParts = Set("year", "month", "day", "hour", "minute", "second")
}
