
package models.sparkworkflow.statement.machinelearning.hyperoptimization

import models.sparkworkflow.statement.{ExtractAndResetColParams, Statement}
import models.sparkworkflow.statement.Statement.Output.{DataFrame, Estimator, Evaluator, Report}
import models.SparkWorkflow.Node

/**
  * @see <a href="https://spark.apache.org/docs/2.3.0/ml-tuning.html#cross-validation">
  */
final class GridSearch(node: Node) extends Statement(node) with ExtractAndResetColParams {

  override def getDataFrameOutput(outputId: Int): Option[String] = {
    val valName = getOutputName(node.orderId, outputId, Report)
    selfOutputs.find(_ == valName)
  }

  override lazy val parentOutputTypes: Vector[String] = Vector(Estimator, DataFrame, Evaluator)

  override lazy val selfOutputTypes: Vector[String] = Vector(Report)

  override lazy val paramKeys: Vector[String] = Vector("number_of_folds")

  override lazy val snippet: String = {

    val Vector(parentEstimator, parentDataFrame, parentEvaluator) = parentOutputs
    val numFolds = get("number_of_folds")
    val reportOutput = selfOutputs(0)

    val estimatorParamOverwrites: Map[String, String] =
      node.parentOutputs.find(_.outputType == Estimator).get.paramOverwrites
    val addGridMethodChain =
      estimatorParamOverwrites
        .map { case (paramName, value) =>
          val Array(valueType, valueStr) = value.split(":")
          val valueForArray = valueType match {
            case "string" => valueStr.split(",").map("\"" + _.trim + "\"").mkString(",")
            case "double" => valueStr.split(",").map(_.trim + ".toDouble").mkString(",")
            case "float" => valueStr.split(",").map(_.trim + ".toFloat").mkString(",")
            case "int" => valueStr.split(",").map(_.trim + ".toInt").mkString(",")
            case "long" => valueStr.split(",").map(_.trim + ".toLong").mkString(",")
            case _ => valueStr
          }

          if (valueType == "boolean") s".addGrid($parentEstimator.$paramName)"
          else s".addGrid($parentEstimator.$paramName, Array($valueForArray))"
        }
        .mkString("")

    s"""
       |def $scopingMethod() = {
       |  import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
       |  import org.apache.spark.sql.types.{StringType, StructField, StructType}
       |  import org.apache.spark.sql.Row
       |
       |  def resetEstimator(): Unit = {
       |    ${extractAndResetColParamsSnippet(parentDataFrame, parentEstimator)}
       |  }
       |  def resetEvaluator(): Unit = {
       |    ${extractAndResetColParamsSnippet(parentDataFrame, parentEvaluator)}
       |  }
       |  resetEstimator()
       |  resetEvaluator()
       |
       |  val paramGrid = new ParamGridBuilder()$addGridMethodChain.build()
       |  val crossValidator =
       |    new CrossValidator()
       |      .setEstimator($parentEstimator)
       |      .setEstimatorParamMaps(paramGrid)
       |      .setEvaluator($parentEvaluator)
       |      .setNumFolds($numFolds)
       |  val cvModel = crossValidator.fit($parentDataFrame)
       |
       |  val sortedData = (paramGrid zip cvModel.avgMetrics).sortBy(_._2)
       |  val dataWithBestMetricOnTop =
       |    if ($parentEvaluator.isLargerBetter) sortedData.reverse else sortedData
       |  val rows = dataWithBestMetricOnTop.map { case (paramMap, metric) =>
       |    val values = paramMap.toSeq.sortBy(_.param.name).map(_.value.toString) :+ metric.toString
       |    Row(values: _*)
       |  }
       |  val rdd = sc.parallelize(rows)
       |
       |  val headers = paramGrid.head.toSeq.map(_.param.name).sorted :+ "metric"
       |  val structFields = headers.map(StructField(_, StringType, true))
       |  val schema = StructType(structFields)
       |
       |  spark.createDataFrame(rdd, schema)
       |}
       |val $reportOutput = $scopingMethod()
       |""".stripMargin
  }
}
