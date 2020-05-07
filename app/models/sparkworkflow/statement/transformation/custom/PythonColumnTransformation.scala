
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.{MixModeColumnParamsInfo, Statement}
import models.sparkworkflow.statement.MixModeColumnParamsInfo.MixModeColumnParamKeys
import models.sparkworkflow.statement.Statement.CodeType
import models.sparkworkflow.statement.Statement.Output.TempView
import models.sparkworkflow.statement.transformation.custom.PythonColumnTransformation._

object PythonColumnTransformation {

  val ExtraKeys: Vector[String] = Vector("code")
}

/**
  * Take a Python function as input to process transformation for one single column.
  *
  * TODO:
  *   1. The `tranformValueFuncDef` in the #snippet is not completed. Please finish.
  *   2. Test and fix potential bugs.
  */
final class PythonColumnTransformation(node: Node) extends Statement(node) with MixModeColumnParamsInfo {

  override def getColParam(key: String): String = super.get(key)

  override lazy val parentOutputTypes: Vector[String] = Vector(TempView)

  override lazy val selfOutputTypes: Vector[String] = Vector(TempView)

  override lazy val paramKeys: Vector[String] = MixModeColumnParamKeys ++ ExtraKeys

  override def codeType = CodeType.PySpark

  private lazy val columnTransformFunctionDef = get(ExtraKeys(0))

  requireNonEmpty(ExtraKeys)

  require(
    columnTransformFunctionDef.replaceAll("\\s", "").startsWith("deftransform_column("),
    s"""
       |The passed-in function has to:
       |  1) be named `transform_column`.
       |  2) take exactly two arguments: the Spark Column to be transformed and the column name currently being transformed.
       |  3) return the transformed column that can cast to the selected target type (parameter).
     """.stripMargin
  )

  override lazy val snippet: String = {

    val p: MixModeColumnParams = getMixModeColumnParams

    val parentTempView = parentOutputs(0)
    val tempViewOutput = selfOutputs(0)

    s"""
       |from pyspark.sql.types import *
       |
       |$columnTransformFunctionDef
       |
       |def extractColumnNames(df, mode, colNamesStr, colTypesStr, indexRangesStr):
       |    schema = df.schema
       |    allColumns = df.columns
       |
       |    dataTypeMappings = {
       |        'StringType' : 'string',
       |        'BolleanType' : 'boolean',
       |        'TimestampType' : 'timestamp',
       |        'ShortType' : 'numeric',
       |        'IntegerType' : 'numeric',
       |        'LongType' : 'numeric',
       |        'FloatType' : 'numeric',
       |        'DoubleType' : 'numeric',
       |        'DecimalType' : 'numeric'
       |    }
       |    colTypes = [x.strip() for x in colTypesStr.split(",")]
       |    fitSparkTypes = [k for k in dataTypeMappings if dataTypeMappings[k] in colTypes]
       |    typeFitColumns = [field.name for field in schema if str(field.dataType) in fitSparkTypes]
       |
       |    colNames = [x.strip() for x in colNamesStr.split(",")]
       |    nameFitColumns = list(set(colNames) & set(allColumns))
       |
       |    rangeFitColumns = []
       |    if indexRangesStr:
       |        rangePairs = [range.split("-") for range in indexRangesStr.split(",")]
       |        for rangePair in rangePairs:
       |            rangePair.sort()
       |            start = int(rangePair[0].strip()) - 1
       |            end = int(rangePair[1].strip())
       |            rangeFitColumns += allColumns[start:end]
       |
       |    totalFitColumns = list(set(typeFitColumns + nameFitColumns + rangeFitColumns))
       |
       |    if mode is "including":
       |        return [col for col in allColumns if col in totalFitColumns]
       |    else:
       |        return [col for col in allColumns if col not in totalFitColumns]
       |
       |
       |def getOutputColName(inputColName):
       |    if "${p.operateOn}" == "one" and "${p.outputMode}" == "append":
       |        return "${p.appendCol}"
       |    else:
       |        return "${p.colPrefix}" + inputColName
       |
       |
       |def genarateNextDF(df, inputColName):
       |    outputColName = getOutputColName(inputColName)
       |
       |    newColumn = transform_column(df[inputColName], inputColName)
       |    return df.withColumn(outputColName, newColumn)
       |
       |def overlapInputColWithOutputCol(df, inputColName):
       |    fieldNames = df.columns
       |    outputColName = getOutputColName(inputColName)
       |    fieldsWithoutInputCol = [name for name in fieldNames if name != inputColName]
       |    fieldsWithoutOutputCol = [name for name in fieldNames if name != outputColName]
       |
       |    return df.select(fieldsWithoutInputCol).withColumnRenamed(outputColName, inputColName).select(fieldsWithoutOutputCol)
       |
       |originDF = spark.sql("select * from $parentTempView")
       |selectedColumns = extractColumnNames(originDF, "${p.filterMode}", "${p.colNames}", "${p.colTypes}", "${p.indexRanges}")
       |
       |appendedDF = reduce(genarateNextDF, selectedColumns, originDF)
       |
       |newDF = reduce(overlapInputColWithOutputCol, selectedColumns, appendedDF) if ("${p.outputMode}" == "replace") else appendedDF
       |
       |newDF.createOrReplaceTempView("$tempViewOutput")
       |$tempViewOutput = "$tempViewOutput"
       |""".stripMargin
  }
}
