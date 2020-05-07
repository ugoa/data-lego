
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.CodeType
import models.sparkworkflow.statement.Statement.Output.TempView

/**
  * Take an R function as input to process transformation.
  */
final class RTransformation(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(TempView)

  override lazy val selfOutputTypes: Vector[String] = Vector(TempView)

  override lazy val paramKeys: Vector[String] = Vector("code")

  override def codeType = CodeType.SparkR

  /** Example:
    * transform <- function(dataframe) {
    *   d1 <- dataframe[, c(2,3,4)]
    *   c2 <- collect(d1)
    *   fun <- function(x){ return(-x) }
    *   d3 <- lapply(c2, fun)
    *   d4 <- createDataFrame(as.data.frame(d3))
    *   return(d4)
    * }
    */
  private lazy val transformFunctionDef: String = get(paramKeys(0))

  require(
    transformFunctionDef.replaceAll("\\s", "").startsWith("transform<-function("),
    "The passed in function must named `transform`, take exactly one argument of type DataFrame " +
      "and return a SparkR `SparkDataFrame`, an R data.frame or data that can be converted to R data.frame " +
      "using data.frame() function. " +
      "See: https://seahorse.deepsense.ai/operations/r_transformation.html"
  )

  override lazy val snippet: String = {

    val parentTempView = parentOutputs(0)
    val tempViewOutput = selfOutputs(0)

    s"""
       |$transformFunctionDef
       |
       |tempDF <- sql("select * from $parentTempView")
       |newDF <- transform(tempDF)
       |
       |createOrReplaceTempView(newDF, "$tempViewOutput")
       |$tempViewOutput <- "$tempViewOutput"
       |""".stripMargin
  }
}
