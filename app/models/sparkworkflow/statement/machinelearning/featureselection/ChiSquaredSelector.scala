
package models.sparkworkflow.statement.machinelearning.featureselection

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.{SetterChainBuilder, Statement}
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Transformer}
import models.sparkworkflow.statement.machinelearning.featureselection.ChiSquaredSelector._

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-features.html#chisqselector">
  */
final class ChiSquaredSelector(node: Node) extends Statement(node) with SetterChainBuilder {

  override def getParam(key: String): String = super.get(key)

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, Transformer)

  override lazy val paramKeys: Vector[String] = ParamSetMethodMappings.map(_._1)

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val setMethodsChain = buildSetMethodChain(ParamSetMethodMappings)
    val Vector(dataFrameOutput, transformerOutput) = selfOutputs

    s"""
       |def $scopingMethod(df: DataFrame): DataFrame = {
       |  val chiSqSelector = new org.apache.spark.ml.feature.ChiSqSelector()$setMethodsChain
       |
       |  val rawLabelCol = chiSqSelector.getLabelCol
       |  val Array(indicator1, value1) = rawLabelCol.split(":")
       |  val labelCol: String =
       |    if (indicator1 == "index") df.schema.fieldNames(value1.toInt)
       |    else if (indicator1 == "name") value1
       |    else value1
       |  chiSqSelector.setLabelCol(labelCol)
       |
       |  val rawFeaturesCol = chiSqSelector.getFeaturesCol
       |  val Array(indicator2, value2) = rawFeaturesCol.split(":")
       |  val featuresCol:String =
       |    if (indicator2 == "index") df.schema.fieldNames(value2.toInt)
       |    else if (indicator2 == "name") value2
       |    else value2
       |  chiSqSelector.setFeaturesCol(featuresCol)
       |
       |  chiSqSelector.fit(df).transform(df)
       |}
       |
       |val $transformerOutput = $scopingMethod _
       |val $dataFrameOutput = $scopingMethod($parentDataFrame)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
     """.stripMargin
  }
}

object ChiSquaredSelector {
  private val ParamSetMethodMappings: Vector[(String, String, String)] = Vector(
    ("selector_type", "setSelectorType", "string"),
    ("num_top_features", "setNumTopFeatures", "integer"),
    ("percentile", "setPercentile", "double"),
    ("fpr", "setFpr", "double"),
    ("label_column", "setLabelCol", "string"),
    ("features_column", "setFeaturesCol", "string"),
    ("output_column", "setOutputCol", "string")
  )
}
