
package services.kafka.consumers

import java.time.Instant

import commands._
import connectors.HiveConnector
import models.AgendaJob
import models.ResultSummary
import services.kafka.KafkaConsumer

import commands.CommandProcessor.STDOutput

/**
  * Consumer that import data from JDBC sources into Hive, should be deprecated by using [[IngestionConsumer]]
  */
final class SqoopToHiveConsumer extends KafkaConsumer {

  override val topic: String = config.getString("topicSqoop")
  override val groupId: String = config.getString("groupIdSqoop")
  override val zookeeperConnect: String = config.getString("producersqoop")

  override def consumeMessage(sqoopJobId: String): Unit = {

    val dao = injector.instanceOf[AgendaJob.DAO]
    val hiveConn = injector.instanceOf[HiveConnector]

    dao.findOneById(sqoopJobId) match {
      case Some(sqoopJob) =>
        var (startAt, exitCode, state, resultLog) = (Instant.now, -1, "", "")

        val previousStatus = sqoopJob.status

        if (sqoopJob.event == "ABORT") {
          dao.updateField(sqoopJobId, "event", "")
          state = "ABORTED"
        } else {
          dao.updateSqoopStatus(sqoopJobId, "RUNNING")
          try {
            val builder = sqoopJob.jobType match {
              case "SAPHANA_HIVE_SQOOP" => SapHanaSqoopCommandBuilder(sqoopJob.sqoopCommandArgs)
              case "POSTGRES_HIVE_SQOOP" => PostgresSqoopCommandBuilder(sqoopJob.sqoopCommandArgs)
              case "MYSQL_HIVE_SQOOP" => MySQLSqoopCommandBuilder(sqoopJob.sqoopCommandArgs)
              case "MSSQL_HIVE_SQOOP" => MSSQLSqoopCommandBuilder(sqoopJob.sqoopCommandArgs)
              case "KDB_HIVE_SQOOP" => KDBSqoopCommandBuilder(sqoopJob.sqoopCommandArgs)
              case _ => throw new Exception(s"Invalid sqoop job type ${sqoopJob.jobType} in ${sqoopJob.id}")
            }

            hiveConn.safeImportIntoTable(builder.targetTable, builder.hiveSchema, stagingTable => {
              val command = builder.renameTargetTable(stagingTable).build
              val stdLog: STDOutput = CommandProcessor.execute(sqoopJob.id, sqoopJob.jobType, command)

              exitCode = stdLog.exitCode
              resultLog = stdLog.output

              stdLog.exitCode
            })

            state = if (exitCode == 0) "COMPLETED" else "FAILED"
          } catch {
            case ex: Exception =>
              state = "FAILED"
              resultLog = ex.toString
              logger.error(resultLog)
          }

          dao.updateSqoopStatus(sqoopJobId, previousStatus, resultLog)
        }

        if (sqoopJob.isScheduled_?) {
          val agendaJobDAO = injector.instanceOf[AgendaJob.DAO]
          val summary = ResultSummary(startAt, endAt = Instant.now, state, resultLog)
          agendaJobDAO.updateSqoopJobSummary(sqoopJobId, summary)
        } else if (sqoopJob.oneTimeImport_?) {
          if (exitCode == 0) dao.updateJobStatus(sqoopJob.id, "IMPORT")
          else dao.updateJobStatus(sqoopJob.id, "IMPORT", "FAILED")
        }
      case None => logger.error(s"Job with Id $sqoopJobId not found in database")
    }
  }
}
