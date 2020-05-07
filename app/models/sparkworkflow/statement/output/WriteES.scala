
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame

/**
  * Write dataframe to Elastic search cluster.
  */
final class WriteES(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector()

  override lazy val paramKeys: Vector[String] = Vector("es_nodes", "es_port", "es_index","es_type")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val parentDataframe = parentOutputs(0)
    val Vector(esNodes, esPort, esIndex, esType) = paramKeys.map(get)

    s"""
       |def $scopingMethod() = {
       |
       |  import org.elasticsearch.spark.sql._
       |
       |  spark.conf.set("es.index.auto.create", "true")
       |  spark.conf.set("es.nodes", "$esNodes")
       |  spark.conf.set("es.port", "$esPort")
       |
       |  $parentDataframe.saveToEs("$esIndex/$esType")
       |}
       |$scopingMethod()
       |""".stripMargin
  }
}




