
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.custom.SQLColumnTransformation._

/**
  * Take Spark SQL expression input to process column transformation.
  */
final class SQLColumnTransformation(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  override lazy val newStageMethodBody: String = ""

  requireNonEmpty(ExtraKeys)

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)

    val Vector(alias, formula) = ExtraKeys.map(get)

    s"""
       |import org.apache.spark.sql.DataFrame
       |def $scopingMethod(df: DataFrame) = {
       |
       |  $UDFRegistrationOp
       |
       |  $ExtractColumnsMethodDefinition
       |  val selectedColumns: Array[String] = $ExtractColumnsOp(df, "${p.filterMode}", "${p.colNames}", "${p.colTypes}", "${p.indexRanges}")
       |
       |  val inputOutputColMappings: Array[(String, String)] =
       |    selectedColumns.map { column =>
       |      val outputColumn =
       |        if ("${p.operateOn}" == "one" && "${p.outputMode}" == "append") "${p.appendCol}"
       |        else "${p.colPrefix}" + column
       |      (column, outputColumn)
       |    }
       |
       |  val appendedDF = (df /: inputOutputColMappings) { (dataFrame, inputOutputColPair) =>
       |    val (inputCol, outputCol) = inputOutputColPair
       |    val newCols = dataFrame.schema.fieldNames :+ s"$formula AS $$outputCol"
       |
       |    dataFrame.selectExpr("*", s"$$inputCol AS $alias").selectExpr(newCols: _*)
       |  }
       |
       |  def overlapInputColWithOutputCol(dataFrame: DataFrame, inputCol: String, outputCol: String): DataFrame = {
       |
       |    val fieldNames = dataFrame.schema.fieldNames
       |    val fieldsWithoutInputCol = fieldNames.filterNot(_ == inputCol)
       |    val fieldsWithoutOutputCol = fieldNames.filterNot(_ == outputCol)
       |
       |    dataFrame
       |      .select(fieldsWithoutInputCol.map(col): _*)
       |      .withColumnRenamed(outputCol, inputCol)
       |      .select(fieldsWithoutOutputCol.map(col): _*)
       |  }
       |
       |  if ("${p.outputMode}" == "replace") {
       |    (appendedDF /: inputOutputColMappings) { (dataFrame, inputOutputMapping) =>
       |      val (inputCol, outputCol) = inputOutputMapping
       |      overlapInputColWithOutputCol(dataFrame, inputCol, outputCol)
       |    }
       |  } else {
       |    appendedDF
       |  }
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |""".stripMargin
  }
}

object SQLColumnTransformation {

  private val ExtraKeys = Vector("input_column_alias", "formula")

  private val UDFRegistrationOp =
    s"""
       |import java.lang.{Double => JavaDouble}
       |
       |def nullSafeTwoParamOp(
       |    f: (Double, Double) => Double): (JavaDouble, JavaDouble) => JavaDouble = {
       |  case (d1, d2) => if (d1 == null || d2 == null) null else f(d1, d2)
       |}
       |
       |def nullSafeOneParamOp(f: (Double) => Double): (JavaDouble) => JavaDouble = {
       |  case (d1) => if (d1 == null) null else f(d1)
       |}
       |spark.udf.register("MINIMUM", nullSafeTwoParamOp(math.min))
       |spark.udf.register("MAXIMUM", nullSafeTwoParamOp(math.max))
       |""".stripMargin
}
