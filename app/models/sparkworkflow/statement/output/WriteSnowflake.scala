
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node

final class WriteSnowflake(node: Node) extends JDBCWriter(node) {

  override def driver: String = "snowflake"
}
