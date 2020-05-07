
package commands

import modules.GlobalContext.injector
import SqoopCommandBuilder.RequiredArgKeys

object SqoopCommandBuilder {

  /**
    * Required options by all sqoop command builder.
    */
  val RequiredArgKeys = Set("connect", "schema_name", "username", "password")
}

/**
  * Abstract class for all command builders that generate a Sqoop import command.
  *
  * @param args Sqoop option-value map where the key in underscore format without the "--" prefix.
  *             For example: "hive_table" -> "orders" will be parsed as "--hive-table orders"
  */
sealed abstract class SqoopCommandBuilder(args: Map[String, String]) {

  /**
    * Custom Arguments by each command builders.
    *
    * @return Sqoop option-value map
    */
  def customArgs: Map[String, String]

  require(
    RequiredArgKeys.forall(args.keySet),
    s"Missing sqoop args: ${RequiredArgKeys.filterNot(args.keySet).mkString(", ")}"
  )

  /**
    * Getter to get value of the option-value map.
    *
    * @param key the key of the map.
    * @return
    */
  protected final def v(key: String): String = args.getOrElse(key, "")

  /**
    * the hive schema where the data to be imported into.
    */
  val hiveSchema: String = if (v("hive_schema").nonEmpty) v("hive_schema") else "default"

  /**
    * The hive table where the data to be imported into.
    * @return the hive table name.
    */
  private var _targetTable = v("hive_table")

  /**
    * Getter of the _targetTable
    * @return The same command builder instance.
    */
  def targetTable: String = _targetTable

  /**
    * Setter of the _targetTable
    * @param newTarget new hive table name.
    * @return The same command builder instance.
    */
  def renameTargetTable(newTarget: String): SqoopCommandBuilder = {
    _targetTable = newTarget
    this
  }

  /**
    * Generate the final Sqoop import command based on the arguments.
    *
    * args structure sample:
    * "args" : {
    *   "connect" : "jdbc:postgresql://drillpoc.corp.io:5432/postgres",
    *   "schema_name" : "public",
    *   "username" : "corp",
    *   "password" : "corppwd",
    *   "table" : "playground",
    *   "hive_schema": "default",
    *   "hive_table" : "nunonuno_5886dc6d695f425ece007638",
    *   "check_column" : "color",
    *   "merge_key" : "equip_id",
    *   "incremental" : "full",
    *   "m": "1"
    * }
    * @return A valid Sqoop import command.
    */
  def build: String = {

    val argString = toArgsString(commonArgs ++ mapColumnHiveArg ++ mapColumnJavaArg ++ customArgs)

    s""" $sshEnv "stty raw -echo; sudo runuser -l hdfs -c 'sqoop import $argString'" """
  }

  private def toArgsString(args: Map[String, String]): String = {
    args.map { case (arg, value) => s"--${arg.replace('_', '-')} $value".trim }.mkString(" ")
  }


  private def sshEnv = {
    val config = injector.instanceOf[play.Configuration]
    val sshUrl: String = config.getString("SSHConnectURL")
    val sshUserName: String = config.getString("SSHConnectUsername")
    s"ssh -tt $sshUserName@$sshUrl"
  }

  private def commonArgs = Map(
    "target_dir" -> s"/apps/hive/warehouse/$targetTable",
    "fields_terminated_by" -> """ \"\\01\" """,
    "hive_table" -> s"$hiveSchema.$targetTable",
    "username" -> v("username"),
    "password" -> v("password"),
    "m" -> (if (v("m").nonEmpty) v("m") else 1.toString),
    "connect" -> v("connect"),
    "hive_import" -> "",
    "create_hive_table" -> "",
    "delete_target_dir" -> ""
  )

  private def mapColumnHiveArg = {
    val formattedValue = v("map_column_hive").filterNot(_.isWhitespace)
    if (formattedValue.isEmpty) Map() else Map("map_column_hive" -> formattedValue)
  }

  private def mapColumnJavaArg = {
    val formattedValue = v("map_column_java").filterNot(_.isWhitespace)
    if (formattedValue.isEmpty) Map() else Map("map_column_java" -> formattedValue)
  }

//  private def fullImportArgs = Map(
//    "hive_import" -> "",
//    "create_hive_table" -> "",
//    "delete_target_dir" -> ""
//  )
//
//  def lastModifiedParams = Set("incremental", "check_column", "merge_key")
//
//  def appendParams = Set("incremental", "check_column")
//
//  private def importTypeArgs: Map[String, String] = {
//    val keys: Set[String] = agendaJob.importMode match {
//      case "lastmodified" => lastModifiedParams
//      case "append" => appendParams
//      case "full" => Set()
//      case _ => Set()
//    }
//    agendaJob.sqoopCommandArgs.filterKeys(keys)
//  }
//
//  private def lastValueArg = {
//    Map("last_value" -> s""" \\\"$lastValue\\\" """)
//  }
//
//  private def modeArgs = if (agendaJob.fullImport_?) fullImportArgs else lastValueArg
}

case class SapHanaSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  private val dataSourceSchema = v("schema_name")
  private val connectArg =
    s"${v("connect")}/currentschema=$dataSourceSchema?zeroDateTimeBehavior=convertToNull"
  private val queryArg =
    s""" \\\"select * from \\\\\\\"$dataSourceSchema\\\\\\\".\\\\\\\"${v("table")}\\\\\\\" WHERE \\\\\\$$CONDITIONS\\\" """

  private val commons = Map(
    "driver" -> "com.sap.db.jdbc.Driver",
    "connect" -> connectArg,
    "query" -> queryArg,
    "hive_drop_import_delims" -> ""
  )
  private val mapperArgs =
    if (v("merge_key").nonEmpty) Map("m" -> 3.toString, "split_by" -> v("merge_key")) else Map("m" -> 1.toString)

  override def customArgs: Map[String, String] = commons ++ mapperArgs
}

case class PostgresSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  override def customArgs: Map[String, String] = Map(
    "table" -> v("table")
  )
}

case class MySQLSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  override def customArgs: Map[String, String] = Map(
    "table" -> v("table")
  )
}

case class MSSQLSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  override def customArgs: Map[String, String] = Map(
    "table" -> v("table")
  )
}

case class KDBSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  override def customArgs = Map(
    "driver" -> "jdbc",
    "query" -> s""" \\\"select * from ${v("table")} WHERE \\\\\\$$CONDITIONS\\\" """,
    "hive_drop_import_delims" -> ""
  )
}

case class RedshiftSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  private val queryArg =
    s""" \\\"SELECT * FROM ${v("schema_name")}.${v("table")} WHERE \\\\\\$$CONDITIONS\\\" """

  override def customArgs: Map[String, String] = Map(
    "query" -> queryArg,
    "driver" -> "com.amazon.redshift.jdbc42.Driver"
  )
}

case class SnowflakeSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  private val queryArg =
    s""" \\\"SELECT * FROM ${v("database")}.${v("schema_name")}.${v("table")} WHERE \\\\\\$$CONDITIONS\\\" """

  override def customArgs = Map(
    "driver" -> "net.snowflake.client.jdbc.SnowflakeDriver",
    "query" -> queryArg
  )
}

case class SalesforceSqoopCommandBuilder(args: Map[String, String]) extends SqoopCommandBuilder(args) {

  private val connectArg =
    s""" \\\"${v("connect")};Database=${v("database")};TransactionMode=ignore\\\" """
  private val queryArg =
    s""" \\\"SELECT * FROM ${v("schema_name")}.${v("table")} WHERE \\\\\\$$CONDITIONS\\\" """

  override def customArgs: Map[String, String] = Map(
    "driver" -> "com.ddtek.jdbc.sforce.SForceDriver",
    "connect" -> connectArg,
    "query" -> queryArg
  )
}


/**
  * Generate a command that kills a sqoop import job process.
  *
  * @param sourceTableName the table name of data source appears in the spark job command
  *                        that is to be killed.
  */
case class AbortSqoopCommandBuilder(sourceTableName: String) {

  def build: String = {
    s"""$sshEnv "
       |sudo pkill -P \\$$(
       |  ps aux |
       |  grep '^root.*sudo runuser.*sqoop import.*$sourceTableName' |
       |  awk '{print \\$$2}' |
       |  head -1
       |)
       |"""".stripMargin.replaceAll("\n", " ")
  }

  private def sshEnv = {
    val config = injector.instanceOf[play.Configuration]
    val sshUrl: String = config.getString("SSHConnectURL")
    val sshUserName: String = config.getString("SSHConnectUsername")
    s"ssh -tt $sshUserName@$sshUrl"
  }
}
