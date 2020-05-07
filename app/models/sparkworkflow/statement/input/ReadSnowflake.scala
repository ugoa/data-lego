
package models.sparkworkflow.statement.input

import models.SparkWorkflow.Node

/**
  * Read data from Snowflake to produce a DataFrame.
  */
final class ReadSnowflake(node: Node) extends JDBCReader(node) {

  override def driver: String = "snowflake"
}
