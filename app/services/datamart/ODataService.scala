
package services.datamart

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import scala.sys.process._
import scala.io.Source

import play.api.libs.json._
import play.api.Logger
import scalaj.http._

import connectors.{ElasticSearchConnector, HiveConnector}
import modules.GlobalContext.injector
import commands.{CommandProcessor, FileUploadCommandBuilder, ODataToElasticSparkCommandBuilder, ODataToHiveCommandBuilder}
import models.DataMart
import utils.Helper.{AppRootPath, mergeBatchFiles}

/**
  * OdataService is to transport the Odata records from Odata server via HTTP requests
  * to designated datasink, which currently are Hive and ElasticSearch.
  * It takes a DataMart.Model as input which holds all the information it needs to proceed.
  *
  * The whole process has the following steps:
  *
  *   1)Download json payload from the url. The downloading process is being executed in batches,
  *     each batch produces a raw json payload file under the same directory. The schema of the
  *     json payload is determined by the Odata server version. Odata has 4 versions so far,
  *     each of which has different json schema. The key is to dig into the json payload by the schema to
  *     get the correct record list. The specfic schema for each version is being listed in
  *     #parseToSparkJSONFormat() method.
  *
  *     The record list will be fetched and parsed to be a list of JSON Objects, which is compatible
  *     with Spark json reader. Ref: https://docs.databricks.com/spark/latest/data-sources/read-json.html
  *
  *   2)Merge all the parsed files into one file.
  *
  *   3)Upload the file to HDFS.
  *
  *   4)Run spark-submit job to load the file data into Hive or to ElasticSearch, according the setting.
  *
  *   5)Delete all the intermidiate tmp files on local and on HDFS.
  */
case class ODataService(dataMart: DataMart.Model) {

  private val tableName = dataMart.tableName
  private val hiveSchema = dataMart.hiveSchema
  private val parentPath = s"$AppRootPath/tmp/uploads"
  private val tempFileName = s"${dataMart.id}.json"
  private val tempFilePath = s"$parentPath/$tempFileName"
  private val tempBatchFileDir = s"$parentPath/${dataMart.id}"
  private val hdfsFilePath = s"/uploads/$tempFileName"

  def run(): Int = {
    try {
      downloadODataPayload()

      val recordCount = mergeDownloadedFiles()
      Logger.info(s"Downloaded $recordCount records. Expected: ${downloadService.entityTotalCount}")

      uploadToTempHDFS()

      dataMart.dataSink match {
        case "HIVE" => importToHive()
        case "ELASTIC" => importToElastic()
        case _ => throw new Exception(s"Invalid Datasink for Odata datamart id: ${dataMart.id}")
      }

    } finally cleanTempFiles()
  }

  private val downloadService: DownloadService = dataMart.authType match {
    case "BASIC" => BasicAuthService
    case "TOKEN" => TokenAuthService
    case "NONE" => NoneAuthService
    case _ => throw new Exception(s"Unknown Auth Type ${dataMart.authType}")
  }

  private def downloadODataPayload(): Unit = {
    Logger.info("Batch downloading odata entities")
    downloadService.download()
  }

  private def mergeDownloadedFiles(): Int = {
    Logger.info("Merging downloaded batch files")

    mergeBatchFiles(tempBatchFileDir, tempFilePath)

    Source.fromFile(tempFilePath).getLines().size
  }

  private def cleanTempFiles(): Unit = {
    val rmBatchFiles = s"rm -rf $tempBatchFileDir"
    rmBatchFiles.!
    val removeMergedFile = s"rm -f $tempFilePath"
    removeMergedFile.!
  }

  private def uploadToTempHDFS(): Unit = {
    Logger.info("Uploading to HDFS")
    val uploadCommand =
      FileUploadCommandBuilder.uploadCommand(tempFileName, tempFilePath, removeHeader = false, hdfsFilePath)
    CommandProcessor.run(tempFileName, "ODATA_UPLOAD_HDFS", uploadCommand)
  }

  private def importToElastic(): Int = {
    Logger.info("Importing to Elastic by Spark")
    val esConn = injector.instanceOf[ElasticSearchConnector]

    esConn.safeImportHDFSIntoIndex(tableName, hdfsFilePath, dataMart.simpleFields, newIndex => {
      val hdfsToESCmd: String = ODataToElasticSparkCommandBuilder(hdfsFilePath, newIndex).build
      CommandProcessor.run(newIndex, "HDFS_ELASTIC_SPARK", hdfsToESCmd)
    })
  }

  private def importToHive(): Int = {
    Logger.info("Importing to Hive by Spark")
    val hiveConn = injector.instanceOf[HiveConnector]

    hiveConn.safeImportIntoTable(tableName, hiveSchema, stagingTable => {
      val cmd = ODataToHiveCommandBuilder(hdfsFilePath, hiveSchema, stagingTable).build
      CommandProcessor.run(stagingTable, "ODATA_HIVE_SPARK", cmd)
    })
  }

  sealed trait DownloadService {

    def request(url: String): HttpRequest

    private val oDataUrl: String = dataMart.url

    lazy val entityTotalCount: Int = {
      val totalCountUrl = s"$oDataUrl/$$count"
      request(totalCountUrl).header("Accept", "text/plain").asString.body.toInt
    }

    def download(): Unit = {
      val ThreadPoolSize = 10
      //in future should be dependent on the __next or odata.nextLink parameter
      val BatchSize = 20
      val excutor: ExecutorService = Executors.newFixedThreadPool(ThreadPoolSize)
      (0 to entityTotalCount by BatchSize).foreach { offset =>
        val downloadUrl = s"$oDataUrl?$$format=json&$$top=$BatchSize&$$skip=$offset"
        val downloadRequest = request(downloadUrl)
        val downloadedFilePath = s"$tempBatchFileDir/batch$offset.json"
        excutor.execute(
          new ODataDownloadTask(
            downloadRequest,
            downloadedFilePath,
            dataMart.odataVersion,
            dataMart.newDataFields
          )
        )
      }

      excutor.shutdown()
      excutor.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
    }
  }

  object BasicAuthService extends DownloadService {

    override def request(url: String): HttpRequest = {
      Http(url).auth(dataMart.username, dataMart.password).option(HttpOptions.allowUnsafeSSL)
    }
  }

  object TokenAuthService extends DownloadService {

    private val token = {
      val request =
        Http(dataMart.tokenUrl)
          .auth(dataMart.appKey, dataMart.appSecret)
          .postForm(Seq("grant_type" -> "client_credentials"))

      val responseBody = request.asString.body
      val jsonBody = Json.parse(responseBody)
      (jsonBody \ "access_token").as[String]
    }

    override def request(url: String): HttpRequest = Http(url).header("Authorization", s"Bearer $token")
  }

  object NoneAuthService extends DownloadService {

    override def request(url: String): HttpRequest = Http(url)
  }
}
