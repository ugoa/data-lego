
package models.sparkworkflow.statement.input

import models.SparkWorkflow.Node

/**
  * Read data from AWS Redshift to produce a DataFrame.
  */
final class ReadRedshift(node: Node) extends JDBCReader(node) {

  override def driver: String = "redshift"
}
