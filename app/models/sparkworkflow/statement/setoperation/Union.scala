
package models.sparkworkflow.statement.setoperation

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame

final class Union(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame, DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector()

  override lazy val snippet: String = {

    val Vector(df1, df2) = parentOutputs
    val dataFrameOutput = selfOutputs(0)

    s"""
       |val $dataFrameOutput = $df1.union($df2)
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}
