
package models.sparkworkflow.statement.input

import models.SparkWorkflow.Node

/**
  * Read data from Postgres to produce a DataFrame.
  */
final class ReadPostgres(node: Node) extends JDBCReader(node) {

  override def driver: String = "postgresql"
}
