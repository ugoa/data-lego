
package connectors

import java.sql._
import javax.inject.Inject

import common.SimpleField
import scala.collection.mutable.ArrayBuffer

import play.api.Logger

import utils.Helper.currentTime

/**
  * Hive JDBC connector to handle all interaction with Hive service.
  * @param config
  */
class HiveConnector @Inject()(config: play.Configuration) extends CommonJDBCService {

  private val DefaultHiveSchema = "default"
  private val DefaultHDFSPath = "/tmp"

  private val HIVEJDBCDriver = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(HIVEJDBCDriver)

  private val HivewriterURL = config.getString("HivewriterURL")
  private val HivewriterPassword = config.getString("HivewriterPassword")

  private var _userName = config.getString("HiveConnectorUsername")

  /**
    * Setter of the user name used as Hive credential.
    * @param newUser new user name.
    * @return new user name.
    */
  def setUserName(newUser: String): String = {
    _userName = newUser
    _userName
  }

  /**
    * Getter of the user name used as Hive credential.
    * @return user name.
    */
  def userName: String = _userName

  /**
    * Overrided method to obtain a Hive JDBC connection.
    * @return A JDBC connection.
    */
  override def getConnection(): Connection = {
    DriverManager.getConnection(HivewriterURL, userName, HivewriterPassword)
  }

  /**
    * Hive SQL Script to list all schemas.
    */
  override val schemaSQL: String = "SHOW schemas"

  /**
    * Drop a hive table.
    *
    * @param tableName The table name to be dropped.
    * @param hiveSchema the table schema.
    */
  def dropHiveTable(tableName: String, hiveSchema: String): Unit = {
    val schema = parseSchema(hiveSchema)
    val sql = s"DROP TABLE IF EXISTS `$schema`.`$tableName`"
    execute(sql)
  }

  override def getTableMetaData(schema: String, tableName: String): List[TableMetaData] = {

    val conn = getConnection()
    val statement = conn.createStatement()

    try {
      val sql = s"DESCRIBE $schema.$tableName"
      val rs: ResultSet = statement.executeQuery(sql)

      val result = ArrayBuffer[TableMetaData]()
      while (rs.next()) { result += TableMetaData(rs.getString(1), rs.getString(2)) }
      rs.close()
      result.toList
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
    * Create a hive table.
    *
    * @param tableName The table name to be created.
    * @param fieldMapping The list of table field names/types.
    * @param delimiter The table delimiter.
    * @param hiveSchema the table schema.
    */
  def createHiveTable(
      tableName: String,
      fieldMapping: Vector[SimpleField],
      delimiter: String,
      hiveSchema: String): Unit = {

    val schema = parseSchema(hiveSchema)
    val hiveCreateSql =
      s"""
         |CREATE TABLE IF NOT EXISTS $schema.$tableName (${fieldArgs(fieldMapping)})
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY '$delimiter'
         |TBLPROPERTIES('serialization.null.format' = '')
         |""".stripMargin.replaceAll("\n", " ")

    execute(hiveCreateSql)
  }

  /**
    * Create an external table where the content located on HDFS.
    *
    * @param tableName The table name to be created.
    * @param fieldMapping The list of table field names/types.
    * @param hiveSchema the table schema.
    */
  def createExternalTable(
      tableName: String,
      fieldMapping: Vector[SimpleField],
      hiveSchema: String): Unit = {

    val schema = parseSchema(hiveSchema)
    val createExternalSql =
      s"""
         |CREATE EXTERNAL TABLE $schema.$tableName (${fieldArgs(fieldMapping)})
         |STORED AS PARQUET LOCATION '$DefaultHDFSPath/$tableName'
      """.stripMargin.replaceAll("\n", " ")

    execute(createExternalSql)
  }

  /**
    * Change table encoding.
    *
    * @param tableName The target table.
    * @param encoding the target encoding.
    * @param hiveSchema the target table format.
    */
  def encodingHiveTable(tableName: String, encoding: String, hiveSchema: String): Unit = {

    val schema = parseSchema(hiveSchema)
    val hiveEncodingSql =
      s"""
         |ALTER TABLE $schema.$tableName
         |SET SERDEPROPERTIES ('serialization.encoding'='$encoding')
         |""".stripMargin.replaceAll("\n", " ")
    execute(hiveEncodingSql)
  }

  /**
    * Load data from HDFS into Hive
    *
    * @param tableName the target table.
    * @param dataPath the full HDFS path where the data located.
    * @param hiveSchema the target hive schema.
    */
  def loadDataIntoTable(tableName: String, dataPath: String, hiveSchema: String): Unit = {

    val schema = parseSchema(hiveSchema)

    val sql = s"LOAD DATA INPATH '$dataPath' OVERWRITE INTO TABLE $schema.$tableName"
    execute(sql)
  }

  /**
    * Create a hive schema.
    *
    * @param schemaName the schema to be created.
    */
  def createSchema(schemaName: String): Unit = {
    val sql = s"CREATE SCHEMA IF NOT EXISTS `$schemaName`"
    execute(sql)
  }

  /**
    * Rename a table.
    *
    * @param oldName old table name.
    * @param newName new table name.
    * @param hiveSchema the hive schema.
    */
  def renameTable(oldName: String, newName: String, hiveSchema: String): Unit = {

    val schema = parseSchema(hiveSchema)
    val sql = s"ALTER TABLE $schema.$oldName RENAME TO $schema.$newName"
    execute(sql)
  }


  /**
    * Safely import data to Hive table.
    * With any given process, first create a temporary table to write data into.
    * If the process completed successfully, delete the old table by the given {@param indexName},
    * rename the temporary table with the given {@param indexName}.
    *
    * @param tableName the table name to be imported.
    * @param hiveSchema the the target hive schema.
    * @param importToStagingTableFn The lambda of custom process for data writing into Hive.
    *
    * @return Exit code. 0 indicate the process completed successfully.
    */

  def safeImportIntoTable(
      tableName: String, hiveSchema: String, importToStagingTableFn: String => Int): Int = {

    val stagingTableName = s"${tableName}_version_$currentTime"
    val schema = parseSchema(hiveSchema)

    try {
      val exitCode = importToStagingTableFn(stagingTableName)
      if (exitCode == 0) {
        dropHiveTable(tableName, schema)
        renameTable(stagingTableName, tableName, schema)
      } else {
        dropHiveTable(stagingTableName, schema)
      }
      exitCode
    } catch {
      case ex: Exception =>
        Logger.error(s"Failed to import into hive table $tableName. ${ex.toString}")
        dropHiveTable(stagingTableName, schema)
        throw ex
    }
  }

  /**
    * Get the the hdfs file name for the external hive table.
    * The resultSet has the following structure:
    *   col_name    | data_type     | comment
    *   -------------------------------------
    *   column1     | string        | ""
    *   column2     | double        | ""
    *   Owner       | hdfs          | null
    *   ...
    *   Location:   | hdfs://zgpoc.corp.io:8020/tmp/sensortabletohive | null
    *   ...
    * @param tableName the hive table
    * @param hiveSchema the hive table schema
    * @return the HDFS file name, return empty string if not found
    */
  private def getHDFSFileName(tableName: String, hiveSchema: String): String = {
    val schema = parseSchema(hiveSchema)
    var name = ""

    val conn = getConnection()
    val statement = conn.createStatement()
    try {
      val sql = s"DESCRIBE FORMATTED $schema.$tableName"
      val rs = statement.executeQuery(sql)
      while (rs.next()) {
        if (rs.getString("col_name").startsWith("Location")) {
          name = rs.getString("data_type").split("/").last
        }
      }
    } catch {
      case _: Exception => Logger.warn("Hive: Conspondent HDFS table not found")
    } finally {
      statement.close()
      conn.close()
    }
    name
  }

  /**
    * Export hive table into local file system directory
    *
    * @param tableName hive table name to be exported
    * @param hiveSchema hive schema of table to be exported
    * @param delimiter delimiter for exported data
    * @return
    *   string of the local path where the exported data being stored to.
    */
  def exportToLocalDir(
      localDir: String, tableName: String, hiveSchema: String, delimiter: String): Unit = {

    val schema = parseSchema(hiveSchema)
    val hiveExportSql =
      s"""
         |INSERT OVERWRITE LOCAL DIRECTORY '$localDir'
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY '$delimiter'
         |SELECT * FROM $schema.$tableName
         |""".stripMargin.replaceAll("\n", " ")
    execute(hiveExportSql)
  }

  private def parseSchema(hiveSchema: String): String = {
    if (hiveSchema.nonEmpty) hiveSchema else DefaultHiveSchema
  }

  private def execute(sql: String): Unit = {

    val conn: Connection = getConnection()
    val statement: Statement = conn.createStatement()
    try {
      Logger.info(s"Hive Executing: $sql")
      statement.execute(sql)
    } finally {
      statement.close()
      conn.close()
    }
  }

  private def fieldArgs(fieldMapping: Vector[SimpleField]): String = {
    fieldMapping.map { case SimpleField(name, fieldType)  => s"`$name` $fieldType" }.mkString(", ")
  }
}
