
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node

/**
  * Write dataframe to Postgres.
  */
final class WritePostgres(node: Node) extends JDBCWriter(node) {

  override def driver: String = "postgresql"
}
