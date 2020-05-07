
package models

import java.time.Instant
import java.util.Date
import javax.inject.Singleton

import com.mongodb.casbah.Imports._

import models.Helper.toUpperCase

object IngestionJob {

  object Model {

    val ValidDataSources = Set("POSTGRES", "MYSQL", "SAPHANA", "ORACLE", "MSSQL", "REDSHIFT", "SNOWFLAKE", "SALESFORCE")
    val ValidDataSinks = Set("HIVE")
  }

  case class Model(
      id: String,
      dataSource: String,
      dataSink: String,
      engine: String,
      sqoopArgs: Map[String, String],
      sparkJDBCArgs: Map[String, String],
      hiveSchema: String,
      hiveTable: String,
      state: String) {

    require(
      Model.ValidDataSources.contains(dataSource),
      s"Invalid data source for IngestionJob $id. Expect one of ${Model.ValidDataSources.mkString(", ")}. Got: `$dataSource`"
    )

    require(
      Model.ValidDataSinks.contains(dataSink),
      s"Invalid data source for IngestionJob $id. Expect one of ${Model.ValidDataSinks.mkString(", ")}. Got: `$dataSink`"
    )

    val jobType: String = s"${dataSource}_${dataSink}_$engine".toUpperCase

    def bySpark: Boolean = engine == "SPARK"

    def bySqoop: Boolean = engine == "SQOOP"

    def toBeCancelled_? : Boolean = state == "ABORTING"

    def isRunning_? : Boolean = state == "RUNNING"

    def isQueued_? : Boolean = state == "QUEUED"
  }

  @Singleton
  class DAO extends CommonDAO[Model] {

    override val mongoCollectionName = "ingestion_jobs"

    override def toEntity(dbObject: DBObject): Model = {

      val v = Helper.v(dbObject, _: String)

      val (dataSource, dataSink) = (toUpperCase(v("data_source")), toUpperCase(v("data_sink")))
      val engine = if (v("engine").isEmpty) "SPARK" else toUpperCase(v("engine"))
      val sqoopArgs = convertToMap(dbObject, "sqoop_args")
      val sparkJDBCArgs = convertToMap(dbObject, "spark_jdbc_args")

      val (hiveSchema, hiveTable) =
        if (engine == "SPARK") {
          (sparkJDBCArgs.getOrElse("hive_schema", ""), sparkJDBCArgs.getOrElse("hive_table", ""))
        } else {
          (sqoopArgs.getOrElse("hive_schema", ""), sqoopArgs.getOrElse("hive_table", ""))
        }

      Model(
        id = dbObject("_id").toString,
        dataSource = dataSource,
        dataSink = dataSink,
        engine = engine,
        sqoopArgs = sqoopArgs,
        sparkJDBCArgs = sparkJDBCArgs,
        hiveSchema = hiveSchema,
        hiveTable = hiveTable,
        state = toUpperCase(v("state"))
      )
    }

    def updateIngestionJobState(objectId: String, state: String): Unit = {
      updateField(objectId, "state", state)
    }

    def setToBeAborted(objectId: String): Unit = {
      updateField(objectId, "state", "ABORTING")
    }

    def updateIngestionJobSummary(id: String, summary: ResultSummary): Unit = {

      val summaryDbo = Map(
        "start_at" -> Date.from(summary.startAt),
        "end_at" -> Date.from(summary.endAt),
        "state" -> summary.state,
        "error" -> summary.error,
        "updated_at" -> Date.from(Instant.now)
      )

      updateFields(id, summaryDbo)
    }
  }
}