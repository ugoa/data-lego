
package services.datamart

import java.time.Instant
import javax.inject.Inject

import akka.actor.{Actor, Props}
import com.google.inject.assistedinject.Assisted
import commands.{CommandProcessor, FileUploadCommandBuilder, HDFSToElasticSparkCommandBuilder}
import connectors.{ElasticSearchConnector, HiveConnector}
import models.DataMart
import modules.GlobalContext.injector
import play.api.Logger
import services.datamart.CSVUploadActor.UploadParams

/**
  * Actor that handles CSV upload service.
  */
class CSVUploadActor extends Actor {
  private val csvUploadServiceFactory = injector.instanceOf[CSVUploadServiceFactory]

  override def receive: Receive = {

    case UploadParams(dataMart, filePath) =>
      try {
        val csvUploadService = csvUploadServiceFactory(dataMart)
        csvUploadService.run(filePath)
      } catch {
        case ex: Exception => play.api.Logger.error(s"Failed to upload CSV. ${ex.toString}")
      }
    case _ =>
  }
}

object CSVUploadActor {
  case class UploadParams(dataMart: DataMart.Model, filePath: String)
  case object Done

  def props: Props = Props(new CSVUploadActor)
}

/**
  * CSVUploadService factory in the purpose of using Guice AssistedInject.
  * @see <a href="https://github.com/google/guice/wiki/AssistedInject">
  */
trait CSVUploadServiceFactory {
  def apply(@Assisted("dataMart") dataUploadId: DataMart.Model): CSVUploadService
}

class CSVUploadService @Inject()(
    @Assisted("dataMart") dataMart: DataMart.Model,
    dao: DataMart.DAO,
    hiveConn: HiveConnector,
    esConn: ElasticSearchConnector) {

  private val dataMartId = dataMart.id
  private val tableName = dataMart.tableName
  private val dataSink = dataMart.dataSink
  private val dataFields = dataMart.simpleFields
  private val delimiter = dataMart.delimiter
  private val encoding = dataMart.characterEncoding
  private val tempFileName = s"$tableName.${if (delimiter == "\\t") "tsv" else "csv"}"
  private val hdfsFilePath = s"/uploads/$tempFileName"

  private def uploadToTempHDFS(filePath: String): Unit = {
    Logger.info("Upload file to HDFS temporarily")
    val uploadCommand =
      FileUploadCommandBuilder.uploadCommand(tempFileName, filePath, dataMart.hasHeader, hdfsFilePath)
    CommandProcessor.run(tableName, "CSV_UPLOAD_HDFS", uploadCommand)
  }

  private def importToHive(): Unit = {
    Logger.info("Importing to Hive")
    val schema = dataMart.hiveSchema
    hiveConn.safeImportIntoTable(tableName, schema, stagingTable => {
      hiveConn.createHiveTable(stagingTable, dataFields, delimiter, schema)
      if (encoding != "default") hiveConn.encodingHiveTable(stagingTable, encoding, schema)
      hiveConn.loadDataIntoTable(stagingTable, hdfsFilePath, schema)
      0
    })
  }

  private def importToElastic(): Int = {
    Logger.info("Importing to ElasticSearch")
    val fieldString = dataFields.map(_.name).mkString(",")
    val fieldTypeString = dataFields.map(_.fieldType).mkString(",")

    esConn.safeImportHDFSIntoIndex(tableName, hdfsFilePath, dataFields, newIndex => {
      val hdfsToESCmd = HDFSToElasticSparkCommandBuilder(
        hdfsFilePath, newIndex, fieldString, fieldTypeString, dataMartId
      ).build
      CommandProcessor.run(tableName, "HDFS_ELASTIC_SPARK", hdfsToESCmd)
    })
  }

  def run(filePath: String): Unit = {
    try {
      dao.updateStatus(dataMartId, "IMPORTING")
      uploadToTempHDFS(filePath)

      if (dataSink == "HIVE") {
        importToHive()
        dao.updateStatus(dataMartId, "IMPORTED")
      } else if (dataSink == "ELASTIC") {
        val exitCode = importToElastic()
        if (exitCode != 0) Logger.error("FAILED to transfer CSV from HDFS to Elastic Search")
        else Logger.info("Successfully upload CSV to Elastic Search")
      } else {
        throw new Exception(s"Unknown datasink `$dataSink`")
      }
    } catch {
      case ex: Exception =>
        Logger.error(s"Failed to upload CSV to $dataSink", ex)
        dao.updateStatus(dataMartId, "IMPORT FAILED", s"${Instant.now}: ${ex.toString}")
    }
  }
}
