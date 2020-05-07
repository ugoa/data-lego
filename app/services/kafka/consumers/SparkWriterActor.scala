
package services.kafka.consumers

import akka.actor.Actor

import commands._
import connectors.{ElasticSearchConnector, HiveConnector}
import models.SqlQuery
import modules.GlobalContext.injector
import services.kafka.consumers.SparkWriterActor.{RunQuery, RunStats}

final class SparkWriterActor extends Actor with SqlQueryRunner {

  override def receive: Receive = {
    case RunQuery(sqlQueryId) => run(sqlQueryId, process)
    case RunStats(statisticId, transformerId) => processStats(statisticId, transformerId)
    case _ =>
  }

  private def process(sqlQuery: SqlQuery.Model): Int = {
    sqlQuery.dataSink match {
      case "HIVE" =>
        processHive(sqlQuery)
      case "ELASTIC" =>
        processElastic(sqlQuery)
      case "CSV" =>
        processCSV(sqlQuery)
      case "MSSQL" =>
        processMSSQL(sqlQuery)
      case _ =>
        throw new Exception(s"Unknown datasink ${sqlQuery.dataSink}")
    }
  }

  private def processHive(sqlQuery: SqlQuery.Model): Int = {
    val hiveConn = injector.instanceOf[HiveConnector]
    val tableName = sqlQuery.destinationTable

    if (sqlQuery.fromAD) hiveConn.setUserName(sqlQuery.runAs)
    hiveConn.safeImportIntoTable(tableName, sqlQuery.tableSchema, stagingTable => {
      val cmd =
        SparkHiveCommandBuilder(
          sqlQuery.id,
          stagingTable,
          sqlQuery.tableSchema,
          sqlQuery.clusterNode,
          sqlQuery.runAs
        ).build
      CommandProcessor.run(sqlQuery.id, sqlQuery.queryType, cmd)
    })
  }

  private def processElastic(sqlQuery: SqlQuery.Model): Int = {
    val esConn = injector.instanceOf[ElasticSearchConnector]
    val esIndex = sqlQuery.destinationTable

    try {

      // The reason of taking 2 steps to write into ES is that we want to explicitly specify
      // mappings for 'date' type in step 1, so that the data field(s) with 'date' type will have
      // the timestamp type in ES, which, otherwise, would be interpreted as 'string' if there are no
      // explicit index mapping.

      val step1Cmd =
        SQLQueryElasticSaprkCommandBuilder(sqlQuery.runAs, sqlQuery.id, esIndex, 1).build

      val step1ExitCode = CommandProcessor.run(sqlQuery.id, sqlQuery.queryType, step1Cmd)
      if (step1ExitCode == 0) {
        val reloadedSQLQuery = sqlQueryDAO.findOneById(sqlQuery.id).get
        val dateFieldMap = reloadedSQLQuery.fieldTypes.filter(_.fieldType == "date")

        esConn.safeImportToIndex(esIndex, dateFieldMap, stagingIndex => {
          val step2Cmd =
            SQLQueryElasticSaprkCommandBuilder(sqlQuery.runAs, sqlQuery.id, stagingIndex, 2).build
          CommandProcessor.run(sqlQuery.id, sqlQuery.queryType, step2Cmd)
        })
      } else {
        step1ExitCode
      }
    } finally esConn.close()
  }

  private def processCSV(sqlQuery: SqlQuery.Model): Int = {
    val hiveConn = injector.instanceOf[HiveConnector]

    if (sqlQuery.fromAD) hiveConn.setUserName(sqlQuery.runAs)
    val cmd =
      CSVToHDFSCommandBuilder(
        sqlQuery.id,
        sqlQuery.fileDestination,
        sqlQuery.clusterNode,
        sqlQuery.runAs
      ).build
    CommandProcessor.run(sqlQuery.id, sqlQuery.queryType, cmd)
  }

  private def processMSSQL(sqlQuery: SqlQuery.Model): Int = {

    val hiveConn = injector.instanceOf[HiveConnector]

    if (sqlQuery.fromAD) hiveConn.setUserName(sqlQuery.runAs)
    val cmd =
      MSSQLDataSinkCommandBuilder(
        sqlQuery.id,
        sqlQuery.connectHost,
        sqlQuery.connectPort,
        sqlQuery.dbName,
        sqlQuery.dbUser,
        sqlQuery.dbPassword,
        sqlQuery.destinationTable,
        sqlQuery.tableSchema,
        sqlQuery.clusterNode,
        sqlQuery.runAs
      ).build
    CommandProcessor.run(sqlQuery.id, sqlQuery.queryType, cmd)
  }

  private def processStats(statisticId: String, transformerId: String): Unit = {
    val cmd = SparkStatsCommandBuilder(statisticId, transformerId).build
    CommandProcessor.run(statisticId, "SPARK_STATS", cmd)
  }
}

object SparkWriterActor {

  case class RunQuery(sqlQueryId: String)

  case class RunStats(statisticId: String, transformerId: String)
}
