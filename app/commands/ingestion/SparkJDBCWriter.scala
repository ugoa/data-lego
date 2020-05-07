
package commands.ingestion

import commands.SparkCommandBuilder

/**
  * spark-submit command builder for SparkJDBCWriter job in 'corpsparkjobs' repo.
  *
  * @param ingestionJobId Id of [[models.IngestionJob.Model]]
  * @param hiveSchema The hive schema where the data is going to be written into by the job.
  * @param hiveTable The hive table where the data is going to be written into by the job.
  * @param dataSource Indicate the JDBC data source where the data will be pulled from.
  */
final case class SparkJDBCWriter(
    ingestionJobId: String,
    hiveSchema: String,
    hiveTable: String,
    dataSource: String) extends SparkCommandBuilder {

  override val entryClass: String = "SparkJDBCWriter"

  override val customOptions: Map[String, String] = Map()

  override val appArgs: Array[String] = Array(ingestionJobId, hiveSchema, hiveTable, dataSource)

  override val runAs: String = ""
}
