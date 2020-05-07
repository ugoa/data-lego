
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.{MultiColumnParamsInfo, Statement}
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.MultiColumnParamsInfo.MultiColumnParamKeys
import models.sparkworkflow.statement.transformation.featureconversion.AssembleVector._

/**
  * @see <a href="https://spark.apache.org/docs/latest/ml-features.html#vectorassembler">
  */
final class AssembleVector(node: Node) extends Statement(node) with MultiColumnParamsInfo {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = ExtraKeys ++ MultiColumnParamKeys

  requireNonEmpty(ExtraKeys)

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val Vector(dataFrameOutput, transformerOutput) = selfOutputs
    val assembledColumn = get(ExtraKeys.head)

    val Vector(filterMode, colNames, colTypes, indexRanges) = MultiColumnParamKeys.map(get)

    s"""
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.ml.feature.VectorAssembler
       |import org.apache.spark.ml.linalg.Vectors
       |
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |  $ExtractColumnsMethodDefinition
       |  val selectedColumns: Array[String] = $ExtractColumnsOp(df, "$filterMode", "$colNames", "$colTypes", "$indexRanges")
       |  val assembler =
       |    new VectorAssembler().setInputCols(selectedColumns).setOutputCol("$assembledColumn")
       |  assembler.transform(df)
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

object AssembleVector {
  private val ExtraKeys = Vector("assembled_column")
}
