
package commands

/**
  * Generate spark-submit command that reads JSON data on HDFS and writes it into Hive table.
  *
  * @param hdfsFilePath the full HDFS path of the JSON file.
  * @param schema The hive schema where the JSON will be written into.
  * @param tableName The hive table where the JSON will be written into.
  */
case class ODataToHiveCommandBuilder(
    hdfsFilePath: String,
    schema: String,
    tableName: String) extends SparkCommandBuilder {

  override val entryClass: String = "ODataToHiveSparkWriter"
  override val customOptions: Map[String, String] = Map()
  override val appArgs: Array[String] = Array(hdfsFilePath, schema, tableName)
  override val runAs: String = ""
}