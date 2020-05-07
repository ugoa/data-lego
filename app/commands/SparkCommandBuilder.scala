
package commands

import modules.GlobalContext.injector
import utils.Helper.{NodeInfo, getNodeList}

/**
  * Trait that for all command builders that generate a spark-submit command.
  */
trait SparkCommandBuilder {

  /**
    * Spark Job owner.
    */
  val runAs: String

  /**
    * Relative class path within `com.corp` package in corpsparkjob project.
    */
  val entryClass: String

  /**
    * Custom options of spark-submit command.
    */
  val customOptions: Map[String, String]

  /**
    * Arguments passed into spark-submit job.
    */
  val appArgs: Array[String]

  protected final val config = injector.instanceOf[play.Configuration]

  /**
    * SSH credential of the cluster where the Spark is running.
    * @return
    */
  protected def commandEnv: String = {
    val sshUrl: String = config.getString("SparkElasticsearchSSH")
    val sshUserName: String = config.getString("SparkElasticsearchSSHUsername")
    s"ssh -tt $sshUserName@$sshUrl "
  }

  private def jobOwner: String = if (runAs.isEmpty) config.getString("DefaultJobOwner") else runAs

  private def optionsStr = customOptions.map { case (arg, value) => s"--$arg $value" }.mkString(" ")

  private def argsStr: String = appArgs.mkString(" ")

  private val sparkjobJarName: String = config.getString("SparkJobJarName")

  /**
    * Main method to generate the spark-submit command.
    * @return A Valid spark-submit command body.
    */
  def build: String = {
    val commandBody =
      s"""
         |SPARK_MAJOR_VERSION=2 spark-submit
         |--files env.properties
         |$optionsStr
         |--class com.corp.$entryClass
         |--driver-class-path $sparkjobJarName
         |$sparkjobJarName $argsStr
         |""".stripMargin.replaceAll("\n", " ")
    s""" $commandEnv "stty raw -echo; sudo runuser -l $jobOwner -c '$commandBody'" """
  }
}

/**
  * Generate spark-submit command that runs the Spark SQL script stored in MongoDB with the document
  * objectId {@param sqlQueryId} and save the result data into Hive table.
  *
  * @param sqlQueryId The id of [[models.SqlQuery.Model]] which refers the document
  *                   that have the Spark SQL script to be running.
  * @param tableName The name of Hive table where the data to be written into.
  * @param hiveSchema The name of Hive schema where the data to be written into.
  * @param clusterNode The node ID of the cluster where the job to be running.
  *                    It only makes sense for EDP stand-alone node.
  * @param runAs The owner of the Spark job.
  */
case class SparkHiveCommandBuilder(
    sqlQueryId: String,
    tableName: String,
    hiveSchema: String,
    clusterNode: String,
    override val runAs: String) extends SparkCommandBuilder {

  override val entryClass: String = "SQLQueryToHiveWriter"

  override val customOptions: Map[String, String] = Map()

  override val appArgs: Array[String] = {
    val schema = if (hiveSchema.nonEmpty) hiveSchema else "default"
    Array(sqlQueryId, tableName, schema)
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

@Deprecated
case class SparkStatsCommandBuilder(
    statisticId: String, transformerId: String) extends SparkCommandBuilder {

  override val entryClass: String = "SQLQueryStatsWriter"

  override val customOptions: Map[String, String] = Map()

  override val appArgs: Array[String] = Array(statisticId, transformerId)

  override val runAs: String = ""
}

/**
  * Generate spark-submit command that runs the Spark SQL script stored in MongoDB with the document
  * objectId {@param sqlQueryId} and save the result data into Elasticsearch index.
  *
  * @param runAs The owner of the Spark job.
  * @param sqlQueryId The id of [[models.SqlQuery.Model]] which refers the document
  *                   that have the Spark SQL script to be running.
  *
  * @param esIndex The ES index where the data to be written to.
  * @param step Value must be 1 or 2.
  *             This is a hack: By default, if without any explicit mapping, the Spark build-in
  *             #saveToEs API will save TimestampType as 'string' in ES, which is not ideal.
  *             So we split the process into 2 steps. Step 1 will run the Spark SQL and get the schema
  *             of result data and save the schema into mongoDB, then scheduler will explicitly create
  *             ES mappings for the timestamp type to disable the auto-converting.
  *             Step 2 will be the final step of actual writting into ES.
  *             Mainly used in EDP. There might be a better solution.
  */
case class SQLQueryElasticSaprkCommandBuilder(
    override val runAs: String,
    sqlQueryId: String,
    esIndex: String,
    step: Int) extends SparkCommandBuilder {

  override val entryClass: String = "SQLQueryToElasticWriter"

  override val customOptions: Map[String, String] = {
    val elasticServerUrl = config.getString("elasticsearchServerFQDN")
    Map("conf" -> s""" spark.es.nodes=\"$elasticServerUrl\" """)
  }

  override val appArgs: Array[String] = Array(sqlQueryId, esIndex, "query", step.toString)
}

/**
  * Generate spark-submit command that read CSV file on HDFS and writes it into Elasticsearch.
  *
  * @param hdfsFilePath The HDFS full path of the CSV file to be read.
  * @param esIndexName The index of ES to be written.
  * @param fieldList The field list of the CSV file, joined by ','
  * @param fieldTypeList The field type list of the CSV file, joind by ','
  * @param dataMartId The id of [[models.DataMart.Model]]
  */
case class HDFSToElasticSparkCommandBuilder(
    hdfsFilePath: String,
    esIndexName: String,
    fieldList: String,
    fieldTypeList: String,
    dataMartId: String) extends SparkCommandBuilder {

  override val entryClass: String = "HDFSToElasticSparkWriter"

  override val customOptions: Map[String, String] = {
    val elasticServerUrl = config.getString("elasticsearchServerFQDN")
    Map("conf" -> s""" spark.es.nodes=\"$elasticServerUrl\" """)
  }
  override val appArgs: Array[String] = Array(
    hdfsFilePath,
    esIndexName,
    format(fieldList),
    format(fieldTypeList),
    dataMartId
  )

  override val runAs: String = ""

  private def format(str: String): String = s""" \"$str\" """
}

/**
  * Generate spark-submit command that read JSON file on HDFS and writes it into Elasticsearch.
  *
  * @param hdfsFilePath The HDFS full path of the JSON file to be read.
  * @param esIndexName The index of ES to be written.
  */
case class ODataToElasticSparkCommandBuilder(
    hdfsFilePath: String,
    esIndexName: String) extends SparkCommandBuilder {

  private val elasticServerUrl = config.getString("elasticsearchServerFQDN")

  override val entryClass: String = "ODataToElasticSparkWriter"
  override val customOptions: Map[String, String] =
    Map("conf" -> s""" spark.es.nodes=\"$elasticServerUrl\" """)
  override val appArgs: Array[String] = Array(hdfsFilePath, esIndexName)
  override val runAs: String = ""
}

/**
  * Generate spark-submit command that runs the Spark SQL script stored in MongoDB with the document
  * objectId {@param sqlQueryId} and save the result data into CSV file on cluster's local filesystem.
  *
  * @param sqlQueryId The id of [[models.SqlQuery.Model]] which refers the document
  *                   that have the Spark SQL script to be running.
  * @param csvFileDestination The CSV path to be written to.
  * @param clusterNode The node ID of the cluster where the job to be running.
  *                    It only makes sense for EDP stand-alone node.
  * @param runAs The owner of the Spark job.
  */
case class CSVToHDFSCommandBuilder(
    sqlQueryId: String,
    csvFileDestination: String,
    clusterNode: String,
    override val runAs: String) extends SparkCommandBuilder {

  private val mongoURL= config.getString("MongoDBURL")
  private val mongoDatabase = config.getString("MongoDBDatabase")

  override val entryClass: String = "CSVToHDFSSparkWriter"
  override val customOptions: Map[String, String] = Map()
  override val appArgs: Array[String] =
    Array(mongoURL, mongoDatabase, sqlQueryId, csvFileDestination)

  override def commandEnv: String = {
    if (clusterNode.isEmpty) {
      super.commandEnv
    } else {
      val nodeInfo: NodeInfo = getNodeList.find(_.nodeName == clusterNode).get
      s"ssh -tt ${nodeInfo.loginUser}@${nodeInfo.host} "
    }
  }
}

/**
  * Generate a command that kills a spark-submit job process.
  * @param tableName the table name appears in the spark job command that is to be killed
  * @param clusterNode the cluster ID indicate the node where the job was running.
  */
case class AbortSparkSubmitCommandBuilder(tableName: String, clusterNode: String) {

  def commandEnv: String = {
    val nodeInfoMappings: Vector[NodeInfo] = getNodeList
    val nodeInfo =
      if (clusterNode.isEmpty) nodeInfoMappings.head
      else nodeInfoMappings.find(_.nodeName == clusterNode).get

    s"ssh -tt ${nodeInfo.loginUser}@${nodeInfo.host} "
  }

  def build: String = {
    s"""$commandEnv "
       |sudo pkill -P \\$$(
       |  ps aux |
       |  grep '^root.*sudo runuser.*spark-submit.*$tableName' |
       |  awk '{print \\$$2}' |
       |  head -1
       |)
       |"""".stripMargin.replaceAll("\n", " ")
  }
}
