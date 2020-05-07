
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.GetFromVector._

/**
  * Extract values from the input columns that is a ml Vector, at vector[index],
  * and append extra columns with those values
  */
final class GetFromVector(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  override lazy val newStageMethodBody: String = ""

  requireInteger(ExtraKeys)

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)

    val indexWithinVector = get(ExtraKeys.head)
    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.sql.types.{DoubleType, StructField}
       |import org.apache.spark.sql.Row
       |import org.apache.spark.ml.linalg.{Vector => SparkVector}
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |  $ExtractColumnsMethodDefinition
       |  val selectedColumns: Array[String] =
       |    $ExtractColumnsOp(df, "${p.filterMode}", "${p.colNames}", "${p.colTypes}", "${p.indexRanges}")
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
       |    val newRDD = dataFrame.rdd.map { case row =>
       |      val vector = row.getAs(inputCol).asInstanceOf[SparkVector]
       |      val newCell = if (vector != null) vector($indexWithinVector) else null
       |      Row.fromSeq(row.toSeq :+ newCell)
       |    }
       |    val newSchema = dataFrame.schema.add(StructField(outputCol, DoubleType, true))
       |    spark.createDataFrame(newRDD, newSchema)
       |  }
       |
       |  def overlapInputColWithOutputCol(
       |      dataFrame: DataFrame, inputCol: String, outputCol: String): DataFrame = {
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

object GetFromVector {

  private val ExtraKeys = Vector("index")
}
