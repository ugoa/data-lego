
package models.sparkworkflow.statement.input

import models.SparkWorkflow.Node

/**
  * Read data from Saleforce to produce a DataFrame.
  */
final class ReadSalesforce(node: Node) extends JDBCReader(node) {

  override def driver: String = "datadirect:sforce"
}
