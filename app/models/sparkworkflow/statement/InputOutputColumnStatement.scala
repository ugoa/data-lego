
package models.sparkworkflow.statement

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.MixModeColumnParamsInfo.MixModeColumnParamKeys

object InputOutputColumnStatement {

  val EmptyInputOutputParamMap: Map[String, String] = MixModeColumnParamKeys.map(_ -> "").toMap
}

/**
  * Base class for all statements that take multiple columns as input and produce dataframe with
  * extra multiple columns calculated based on the input.
  */
abstract class InputOutputColumnStatement(node: Node) extends Statement(node) with MixModeColumnParamsInfo {

  def extraKeys: Vector[String]

  def newStageMethodBody: String

  override final def getColParam(key: String): String = super.get(key)

  override final lazy val parentOutputTypes = Vector(DataFrame)

  override final lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override final lazy val paramKeys: Vector[String] = extraKeys ++ MixModeColumnParamKeys

  protected final lazy val p: MixModeColumnParams = getMixModeColumnParams

  override lazy val snippet: String = {

    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val parentDataFrame = parentOutputs(0)

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.ml.Pipeline
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |
       |  $ExtractColumnsMethodDefinition
       |  val selectedColumns: Array[String] =
       |    $ExtractColumnsOp(df, "${p.filterMode}", "${p.colNames}", "${p.colTypes}", "${p.indexRanges}")
       |
       |  val inputOutputColMappings: Array[(String, String)] =
       |    selectedColumns.map { inputCol =>
       |      val outputCol =
       |        if ("${p.operateOn}" == "one" && "${p.outputMode}" == "append") "${p.appendCol}"
       |        else "${p.colPrefix}" + inputCol
       |      (inputCol, outputCol)
       |    }
       |
       |  def newStage() = {
       |    $newStageMethodBody
       |  }
       |  val newOutputColStages = inputOutputColMappings.map { case (inputCol, outputCol) =>
       |    newStage().setInputCol(inputCol).setOutputCol(outputCol)
       |  }
       |
       |  val pipeline = new Pipeline().setStages(newOutputColStages)
       |  val appendedDF = pipeline.fit(df).transform(df)
       |
       |  def overlapInputColWithOutputCol(dataFrame: DataFrame, inputCol: String, outputCol: String): DataFrame = {
       |
       |    val fieldNames: Array[String] = dataFrame.schema.fieldNames
       |    val fieldsWithoutInputCol: Array[String] = fieldNames.filterNot(_ == inputCol)
       |    val fieldsWithoutOutputCol: Array[String] = fieldNames.filterNot(_ == outputCol)
       |
       |    dataFrame
       |      .select(fieldsWithoutInputCol.map(col): _*)
       |      .withColumnRenamed(outputCol, inputCol)
       |      .select(fieldsWithoutOutputCol.map(col): _*)
       |  }
       |
       |  if ("${p.outputMode}" == "replace") {
       |    (appendedDF /: inputOutputColMappings) { case (dataFrame, (inputCol, outputCol)) =>
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
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

