
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node

/**
  * Write dataframe to Salesforce.
  */
final class WriteSalesforce(node: Node) extends JDBCWriter(node) {

  override def driver: String = "datadirect:sforce"
}
