
package services.kafka.consumers

import scala.sys.process._

import play.api.Logger

import commands.CommandProcessor
import connectors.HiveConnector
import models.HiveExport
import services.kafka.KafkaConsumer
import utils.Helper.mergeBatchFiles
import services.kafka.consumers.HiveToCSVConsumser.RootTmpDir

/**
  * Consumer that export Hive table as CSV to the assigned location.
  *
  * Step 1: Export Hive table to /tmp/[table] directory on the server where Hive host is.
  * Step 2: Transport the directory from remote Hive host server to local /tmp/[table] directory.
  * Step 3: Merge all the content into one single CSV file.
  * Step 4: Prepend header into the CSV file.
  * Step 5 Upload the CSV file to the destination where the user assigned.
  */
final class HiveToCSVConsumser extends KafkaConsumer {

  override val topic: String = config.getString("kafka.hiveToCSV.topic")
  override val groupId: String = config.getString("kafka.hiveToCSV.group")
  override val zookeeperConnect: String = config.getString("kafka.hiveToCSV.zkConnect")

  private val scpUser = config.getString("PrimaryUserRole")
  private val hiveServer = config.getString("servers.hive")

  override def consumeMessage(hiveExportId: String): Unit = {

    val dao = injector.instanceOf[HiveExport.DAO]

    dao.findOneById(hiveExportId) match {
      case Some(hiveExport) =>
        val (table, schema) = (hiveExport.tableName, hiveExport.hiveSchema)
        val sshConf = hiveExport.protocolConfig

        val hiveLocalDir = s"$RootTmpDir/$table"
        val localDir = s"$RootTmpDir/${table}_local"
        val targetFile = s"$RootTmpDir/${table}_local.csv"
        val destDir = s""""${sshConf.destDir.replaceAll(" ","\\\\ ")}""""
        val remoteDestFile = s"${sshConf.user}@${sshConf.host}:$destDir/$table.csv"

        var exitCode = -1
        val hiveConn = injector.instanceOf[HiveConnector]

        try {
          dao.updateStatus(hiveExportId, "EXPORTING")

          // Step 1
          val delimiter = hiveExport.delimiter
          hiveConn.exportToLocalDir(hiveLocalDir, table, schema, delimiter)

          // Step 2
          val scpHiveServerToLocalCommand =
            s"scp -o StrictHostKeyChecking=no -r $scpUser@$hiveServer:$hiveLocalDir $localDir"
          exitCode = CommandProcessor.run(table, "HIVE_SERVER_TO_LOCAL", scpHiveServerToLocalCommand)
          if (exitCode != 0) throw new Exception("Failed to fetch folders from Hive server to local")

          // Step 3
          exitCode = mergeBatchFiles(localDir, targetFile)
          if (exitCode != 0) throw new Exception("Failed to merge raw files into single CSV file")

          // Step 4
          val columns =
            hiveConn.getTableMetaData(table, schema).map(_.columnName).mkString(delimiter)
          val addHeaderCommand = s"""sudo sed -i '1i $columns' $targetFile"""
          exitCode = CommandProcessor.run(table, "PREPEND_CSV_HEADER", addHeaderCommand)

          // Step 5
          dao.updateStatus(hiveExportId, "UPLOADING")
          val uploadWithSSHCommand =
            s"""
               |sudo sshpass -p '${sshConf.password}'
               |scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
               |$targetFile $remoteDestFile
               |""".stripMargin.replaceAll("\n", " ")
          exitCode = CommandProcessor.run(table, "LOCAL_REMOTE_SCP", uploadWithSSHCommand)
          if (exitCode != 0) throw new Exception("Failed to upload CSV to destination")

          dao.updateStatus(hiveExportId, "COMPLETED")
          Logger.info(s"Uploading completed to $remoteDestFile as CSV")
        } catch {
          case ex: Exception =>
            Logger.error(s"Failed to export Hive table $schema.$table as CSV. ${ex.toString}")
            dao.updateStatus(hiveExportId, "FAILED", ex.toString)
        } finally {
          val rmLocalDir = s"rm -rf $localDir"
          rmLocalDir.!
          val rmTargetFile = s"rm -f $targetFile"
          rmTargetFile.!
          val changeLocalDirOwner = s"sudo chown $scpUser -R $hiveLocalDir"
          changeLocalDirOwner.!
          val rmHiveLocalDir = s"rm -rf $hiveLocalDir"
          rmHiveLocalDir.!
        }
      case None =>
        logger.error(s"HiveExport with Id $hiveExportId not found in database")
    }
  }
}

object HiveToCSVConsumser {

  val RootTmpDir = "/tmp"
}
