
package commands

import utils.Helper.{NodeInfo, getNodeList}

/**
  * Case class to generate spark-submit command that write the resulted data of the sql query into
  * the targeted Sql Server database/table.
  *
  * @param sqlQueryId Id of the model [[models.SqlQuery]].
  * @param connectHost Host of the Sql Server.
  * @param port Port of the Sql Server.
  * @param dbName The name of the Sql Server database
  * @param dbUser The user that has access to the Sql Server database.
  * @param dbPassword The password with the user.
  * @param tableName The Sql Server table that to be written into.
  * @param schema The target Sql Server schema.
  * @param clusterNode Indicate within which node should the spark-submit job be running.
  *                    It only makes sense for enabling parallel job processing in stand-alone mode,
  *                    and should be deprecated.
  * @param runAs The job owner, 'hdfs' by default.
  */
case class MSSQLDataSinkCommandBuilder(
    sqlQueryId: String,
    connectHost: String,
    port: String,
    dbName: String,
    dbUser: String,
    dbPassword: String,
    tableName: String,
    schema: String,
    clusterNode: String,
    override val runAs: String) extends SparkCommandBuilder {

  override val entryClass: String = "SQLQueryToMSSQLSparkWriter"
  override val customOptions: Map[String, String] = Map(
    "driver-class-path" -> "mssql-jdbc-6.4.0.jre8.jar"
  )
  override val appArgs: Array[String] = {
    Array(connectHost, port, dbName, dbUser,dbPassword, sqlQueryId, tableName, schema)
  }

  override def commandEnv: String = {
    if (clusterNode.isEmpty) {
      super.commandEnv
    } else {
      val nodeInfo: NodeInfo = getNodeList.find(_.nodeName == clusterNode).get
      s"ssh -tt ${nodeInfo.loginUser}@${nodeInfo.host} "
    }
  }
}
