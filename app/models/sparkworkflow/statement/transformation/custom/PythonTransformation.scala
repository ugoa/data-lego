
package models.sparkworkflow.statement.transformation.custom

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.CodeType
import models.sparkworkflow.statement.Statement.Output.TempView

/**
  * Take python code as input to process transformation.
  */
final class PythonTransformation(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(TempView)

  override lazy val selfOutputTypes: Vector[String] = Vector(TempView)

  override lazy val paramKeys: Vector[String] = Vector("code")

  override def codeType = CodeType.PySpark

  override lazy val snippet: String = {

    val tranformFunctionDef = get(paramKeys(0))

    val parentTempView = parentOutputs(0)
    val tempViewOutput = selfOutputs(0)

    s"""
       |$tranformFunctionDef
       |
       |temp_df = spark.sql("select * from $parentTempView")
       |new_df = transform(temp_df)
       |
       |new_df.createOrReplaceTempView("$tempViewOutput")
       |$tempViewOutput = "$tempViewOutput"
       |""".stripMargin
  }
}
