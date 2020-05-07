
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node

/**
  * Write dataframe to Redshift.
  */
final class WriteRedshift(node: Node) extends JDBCWriter(node) {

  override def driver: String = "redshift"
}
