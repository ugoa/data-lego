
package services.kafka.consumers

import java.time.Instant

import commands._
import commands.ingestion.SparkJDBCWriter
import connectors.HiveConnector
import models.ResultSummary
import models.IngestionJob.{DAO, Model}
import services.kafka.KafkaConsumer

import commands.CommandProcessor.STDOutput

/**
  * Consumer that import from external source into Hive table. It supports 2 mechanisms: Sqoop importing
  * and Spark JDBC importing. Which one to choose depends on the user.
  */
final class IngestionConsumer extends KafkaConsumer {

  override val topic: String = config.getString("kafka.ingestion.topic")
  override val groupId: String = config.getString("kafka.ingestion.group")
  override val zookeeperConnect: String = config.getString("kafka.ingestion.zkConnect")

  private lazy val hiveConn = injector.instanceOf[HiveConnector]

  override def consumeMessage(ingestionJobId: String): Unit = {

    val dao = injector.instanceOf[DAO]

    dao.findOneById(ingestionJobId) match {
      case Some(ingestionJob) =>

        var (startAt, state, errorMessage) = (Instant.now, "", "")
        try {

          if (ingestionJob.toBeCancelled_?) {
            dao.updateIngestionJobState(ingestionJobId, "ABORTED")
            return
          }

          dao.updateIngestionJobState(ingestionJobId, "RUNNING")

          val stdout: STDOutput =
            if (ingestionJob.bySqoop) {
              ingestWithSqoop(ingestionJob)
            } else {
              ingestWithSpark(ingestionJob)
            }

          state = if (stdout.exitCode == 0) "COMPLETED" else "FAILED"
        } catch {
          case ex: Exception =>
            state = "FAILED"
            errorMessage = ex.toString
            logger.error(errorMessage)
        }

        val summary = ResultSummary(startAt, endAt = Instant.now, state, errorMessage)
        injector.instanceOf[DAO].updateIngestionJobSummary(ingestionJobId, summary)

      case None =>
        logger.error(s"Job with Id $ingestionJobId not found in database")
    }
  }


  private def ingestWithSqoop(ingestionJob: Model): STDOutput = {

    val builder = ingestionJob.jobType match {
      case "SAPHANA_HIVE_SQOOP" => SapHanaSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "POSTGRES_HIVE_SQOOP" => PostgresSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "MYSQL_HIVE_SQOOP" => MySQLSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "MSSQL_HIVE_SQOOP" => MSSQLSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "KDB_HIVE_SQOOP" => KDBSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "REDSHIFT_HIVE_SQOOP" => RedshiftSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "SNOWFLAKE_HIVE_SQOOP" => SnowflakeSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case "SALESFORCE_HIVE_SQOOP" => SalesforceSqoopCommandBuilder(ingestionJob.sqoopArgs)
      case _ =>
        throw new Exception(s"Invalid sqoop job type ${ingestionJob.jobType} in ${ingestionJob.id}")
    }


    var exitCode = -1
    var outout = ""

    hiveConn.safeImportIntoTable(builder.targetTable, builder.hiveSchema, stagingTable => {
      val cmd = builder.renameTargetTable(stagingTable).build
      val stdout: STDOutput = CommandProcessor.execute(ingestionJob.id, ingestionJob.jobType, cmd)

      exitCode = stdout.exitCode
      outout = stdout.output

      stdout.exitCode
    })

    STDOutput(exitCode, outout)
  }

  private def ingestWithSpark(ingestionJob: Model): STDOutput = {

    var exitCode = -1
    var outout = ""

    hiveConn.safeImportIntoTable(ingestionJob.hiveTable, ingestionJob.hiveSchema, stagingTable => {
      val cmd =
        SparkJDBCWriter(
          ingestionJob.id,
          ingestionJob.hiveSchema,
          stagingTable,
          ingestionJob.dataSource
        ).build

      val stdout: STDOutput = CommandProcessor.execute(ingestionJob.id, ingestionJob.jobType, cmd)

      exitCode = stdout.exitCode
      outout = stdout.output

      stdout.exitCode
    })

    STDOutput(exitCode, outout)
  }
}