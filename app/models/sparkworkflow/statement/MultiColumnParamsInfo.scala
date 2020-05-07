
package models.sparkworkflow.statement
import models.sparkworkflow.statement.MultiColumnParamsInfo.SupportedColTypes

object MultiColumnParamsInfo {

  val MultiColumnParamKeys: Vector[String] = Vector(
    "filter_mode",
    "filter_column_names",
    "filter_column_types",
    "filter_column_index_ranges"
  )

  val SupportedColTypes = Set("numeric", "string", "timestamp", "boolean")
}

/**
  * Trait that extract and validate params of the statement which take multiple columns as input.
  */
trait MultiColumnParamsInfo {

  protected val statement: String = this.getClass.getSimpleName

  lazy val ExtractColumnsOp: String = "extractColumns"

  lazy val ExtractColumnsMethodDefinition: String =
    s"""
       |import org.apache.spark.sql.DataFrame
       |
       |def $ExtractColumnsOp(
       |    df: DataFrame,
       |    mode: String,
       |    colNamesStr: String,
       |    colTypesStr: String,
       |    indexRangesStr: String): Array[String] = {
       |
       |  val schema = df.schema
       |  val allColumns: Array[String] = df.schema.fieldNames
       |
       |  val colTypes: Array[String] = colTypesStr.split(",").map(_.trim)
       |  val dataTypeMappings = Map(
       |    "StringType" -> "string",
       |    "BolleanType" -> "boolean",
       |    "TimestampType" -> "timestamp",
       |    "ShortType" -> "numeric",
       |    "IntegerType" -> "numeric",
       |    "LongType" -> "numeric",
       |    "FloatType" -> "numeric",
       |    "DoubleType" -> "numeric",
       |    "DecimalType" -> "numeric"
       |  )
       |  val fitSparkTypes: Array[String] =
       |    dataTypeMappings.filter { case (k, v) => colTypes.contains(v) }.keys.toArray
       |  val typeFitColumns: Array[String] =
       |    schema
       |      .collect { case field if fitSparkTypes.contains(field.dataType.toString) => field.name }
       |      .toArray
       |
       |  val colNames: Array[String] = colNamesStr.split(",").map(_.trim)
       |  val nameFitColumns: Array[String] = colNames.intersect(allColumns)
       |
       |  val rangeFitColumns: Array[String] =
       |    if (indexRangesStr == "") {
       |      Array()
       |    } else { // String in a form as "1-2,5-7,10-20,30-68"
       |      indexRangesStr.split(",").flatMap { range: String =>
       |        val Array(start, end) = range.split("-").map(_.trim.toInt).sorted
       |        allColumns.slice(start - 1, end)
       |      }
       |    }
       |
       |  val totalFitColumns = (typeFitColumns ++ nameFitColumns ++ rangeFitColumns).distinct
       |
       |  if (mode == "including") totalFitColumns else allColumns.diff(totalFitColumns)
       |}
       """.stripMargin

  class MultiColumnParams(
      filterMode: String,
      colNames: String,
      colTypes: String,
      indexRanges: String) {

    require(indexRangeValid_?, s"$statement: Incorrect Range format `$indexRanges`")

    require(
      colTypesValid_?,
      s"$statement: Column types must be subset of ${SupportedColTypes.mkString(", ")}, got: $colTypes"
    )

    /**
      * Valid index range string transformation:
      *
      *   "1-5,2-7,10-12"
      *   -> Array("1-5", "2-7", "10-12")
      *   -> Array((1, 5), (2, 7), (10, 12))
      *
      * Returns false if such transformation throws exception.
      */
    private def indexRangeValid_? : Boolean = {
      if (indexRanges == "") return true
      try {
        val rangePairs = indexRanges.split(",")
        rangePairs.foreach { rangeStr =>
          val Array(start, end) = rangeStr.split("-").map(_.trim.toInt).sorted
          if (start < 1) throw new Exception("start index must NOT smaller than 1")
        }
        true
      } catch { case _: Exception => false }
    }

    /**
      * Valid col type example: 'string,boolean,timestamp'
      */
    private def colTypesValid_? : Boolean = {
      if (colTypes == "") return true
      try {
        val types: Array[String] = colTypes.split(",")
        types.forall(SupportedColTypes.contains)
      } catch { case _: Exception => false }
    }
  }
}

