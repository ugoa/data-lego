
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.{MixModeColumnParamsInfo, Statement}
import models.sparkworkflow.statement.MixModeColumnParamsInfo.MixModeColumnParamKeys
import models.sparkworkflow.statement.Statement.CodeType
import models.sparkworkflow.statement.Statement.Output.TempView
import models.sparkworkflow.statement.transformation.custom.PythonColumnTransformation._

object RColumnTransformation {

  val ExtraKeys: Vector[String] = Vector("code")
}

/**
  * Take an R function as input to process transformation for one single column.
  */
final class RColumnTransformation(node: Node) extends Statement(node) with MixModeColumnParamsInfo {

  override def getColParam(key: String): String = super.get(key)

  override lazy val parentOutputTypes: Vector[String] = Vector(TempView)

  override lazy val selfOutputTypes: Vector[String] = Vector(TempView)

  override lazy val paramKeys: Vector[String] = MixModeColumnParamKeys ++ ExtraKeys

  override def codeType = CodeType.SparkR

  private lazy val columnTransformFunctionDef = get(ExtraKeys(0))

  requireNonEmpty(ExtraKeys)

  require(
    columnTransformFunctionDef.replaceAll("\\s", "").startsWith("transform.column<-function("),
    s"""
       |The passed-in function has to:
       |  1) be named `transform.column`.
       |  2) take exactly two arguments: the Spark Column to be transformed and the column name currently being transformed.
       |  3) return the transformed column that can cast to the selected target type (parameter).
     """.stripMargin
  )

  override lazy val snippet: String = {

    val p: MixModeColumnParams = getMixModeColumnParams

    val parentTempView = parentOutputs(0)
    val tempViewOutput = selfOutputs(0)

    s"""
       |$columnTransformFunctionDef
       |
       |extractColumnNames <- function(df, mode, colNamesStr, colTypesStr, indexRangesStr) {
       |
       |  allDFColNames <- names(df)
       |
       |  colNames <- unlist(strsplit(colNamesStr, ","))
       |  nameFitColumns <- intersect(allDFColNames, colNames)
       |
       |  colTypes <- unlist(strsplit(colTypesStr, ","))
       |  fitSparkTypes <- c()
       |  for (t in colTypes) {
       |    if (t == "string") {
       |      fitSparkTypes <- c(fitSparkTypes, "StringType")
       |    } else if (t == "boolean") {
       |      fitSparkTypes <- c(fitSparkTypes, "BolleanType")
       |    } else if (t == "timestamp") {
       |      fitSparkTypes <- c(fitSparkTypes, "TimestampType")
       |    } else if (t == "numeric") {
       |      fitSparkTypes <- c(fitSparkTypes, "ShortType", "IntegerType", "LongType", "FloatType", "DoubleType", "DecimalType")
       |    }
       |  }
       |  fitSparkTypes <- unique(fitSparkTypes)
       |  typeFitColumns <- c()
       |  for (field in schema(df)$$fields()) {
       |    type <- field$$dataType.toString()
       |    if (any(fitSparkTypes == type)) {
       |      typeFitColumns <- c(typeFitColumns, field$$name())
       |    }
       |  }
       |
       |  colRanges <- unlist(strsplit(indexRangesStr, ","))
       |  rangeFitColumns <- c()
       |  for (rangeStr in colRanges) {
       |    range <- unlist(strsplit(rangeStr, "-"))
       |    start <- range[1]
       |    end <- range[2]
       |    rangeFitColumns <- c(rangeFitColumns, allDFColNames[start:end])
       |  }
       |  na.omit(rangeFitColumns)
       |
       |  totalFitColumns <- unique(c(nameFitColumns, typeFitColumns, rangeFitColumns))
       |
       |  if (mode == "including") {
       |    return(totalFitColumns)
       |  } else {
       |    return(setdiff(allDFColNames, totalFitColumns))
       |  }
       |}
       |
       |getOutputColName <- function(inputColName) {
       |  if ("${p.operateOn}" == "one" && "${p.outputMode}" == "append") {
       |    return("${p.appendCol}")
       |  } else {
       |    return(paste("${p.colPrefix}", inputColName, sep=""))
       |  }
       |}
       |
       |overlapInputColWithOutputCol <- function(df, inputColName, outputColName) {
       |  allDFColNames <- names(df)
       |  colNamesWithoutInputCol <- allDFColNames[allDFColNames != inputColName]
       |  colNamesWithoutOutputCol <- allDFColNames[allDFColNames != outputColName]
       |
       |  newDF <- select(
       |    withColumnRenamed(
       |      select(
       |        df,
       |        colNamesWithoutInputCol
       |      ),
       |      outputColName,
       |      inputColName
       |    ),
       |    colNamesWithoutOutputCol
       |  )
       |  return(newDF)
       |}
       |
       |generateNextDF <- function(df, inputColName) {
       |  outputColName <- getOutputColName(inputColName)
       |  newColumn <- transform.column(df[[inputColName]], inputColName)
       |  return(withColumn(df, outputColName, newColumn))
       |}
       |
       |originDF <- sql("select * from $parentTempView")
       |selectedColumns <- extractColumnNames(originDF, "${p.filterMode}", "${p.colNames}", "${p.colTypes}", "${p.indexRanges}")
       |
       |appendedDF <- originDF
       |for (colName in selectedColumns) {
       |  appendedDF <- generateNextDF(appendedDF, colName)
       |}
       |
       |resultDF <- if ("${p.outputMode}" == "replace") {
       |  tempDF <- appendedDF
       |  for (inputColName in selectedColumns) {
       |    outputColName <- getOutputColName(inputColName)
       |    tempDF <- overlapInputColWithOutputCol(tempDF, inputColName, outputColName)
       |  }
       |  tempDF
       |} else {
       |  appendedDF
       |}
       |
       |createOrReplaceTempView(resultDF, "$tempViewOutput")
       |$tempViewOutput <- "$tempViewOutput"
       |""".stripMargin
  }
}
