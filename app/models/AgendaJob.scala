
package models

import java.util.Date
import javax.inject.Singleton

import com.mongodb.casbah.Imports._

import modules.GlobalContext.injector
import services.schedule.QuartzScheduler

object AgendaJob {

  object Model {

    /**
      * All supported event-status mapping for scheduling jobs.
      */
    val EventStatusMappings =
      Map(
        "IMPORT" -> "IMPORTED",
        "SCHEDULE" -> "SCHEDULED",
        "PAUSE" -> "PAUSED",
        "CANCEL" -> "CANCELLED",
        "ABORT" -> "ABORTED"
      )
  }

  /**
    * The Entity of AgendaJob
    *
    * @param id The id of the entity
    * @param sqlQueryId The foreign id that refers to the [[models.SqlQuery.Model.id]]
    * @param dataMartId The foreign id that refers to the [[models.DataMart.Model.id]]
    * @param workflowId The foreign id that refers to the [[models.SparkWorkflow.Model.id]]
    * @param jobType The type decription of the job, defined by `dataSource` and `dataSink`
    * @param dataSource The data source of the job.
    * @param dataSink The data destination of the job.
    * @param status The job scheduling status.
    * @param event The event set by front-end to indicate which process to execute.
    *              TODO: Decouple the Agendajob with Sqoop job and remove the event fields.
    * @param frequency the job frequency description.
    * @param sqoopCommandArgs Sqoop command options. Bad legacy design.
    *                         TODO: We also use agendajob for executing one-time sqoop importing,
    *                         because the sqoop jobs was, originally, the only type of job that needs
    *                         to be scheduled and the agendajob was implemented for it. But now the
    *                         Agendajob also schedules SqlQuery, DataMart and Workflow. It doesn't fit
    *                         the situation anymore.
    *                         The new data model for sqoop one-time importing is [[models.IngestionJob.Model]],
    *                         please align with front-end to make the transition.
    */
  case class Model(
      id: String,
      sqlQueryId: String,
      dataMartId: String,
      workflowId: String,
      jobType: String,
      dataSource: String,
      dataSink: String,
      status: String,
      event: String,
      frequency: String,
      sqoopCommandArgs: Map[String, String]) {

    private val incremental = sqoopCommandArgs.getOrElse("incremental", "").toLowerCase

    val group: String = s"${jobType}_GROUP".toUpperCase

    val sourceTableName: String = sqoopCommandArgs.getOrElse("table", "INVALID_SOURCE_TABLE")

    val tableName: String = sqoopCommandArgs.getOrElse("hive_table", "INVALID_HIVE_TABLE")

    val hiveSchema: String = sqoopCommandArgs.getOrElse("hive_schema", "")

    val importMode: String = if (fullImport_?) "full" else incremental

    def oneTimeImport_? : Boolean = event == "IMPORT"

    def fullImport_? : Boolean = event == "IMPORT" || incremental == "full"

    def isSqlQueryJob_? : Boolean = jobType.startsWith("SQLQUERY")

    def isDataMartJob_? : Boolean = jobType.startsWith("DATAMART")

    def isWorkflowJob_? : Boolean = jobType.startsWith("WORKFLOW")

    def isScheduled_? : Boolean = status == "SCHEDULED"

    def toBeCancelled_? : Boolean = status == "CANCELLING" || status == "ABORTING"

    def isRunning_? : Boolean = status == "RUNNING"

    def isQueued_? : Boolean = status == "QUEUED" || status == "IMPORT"

    def eventValid_? : Boolean = {
      Model.EventStatusMappings.contains(event) &&
        jobType.nonEmpty &&
        Array("", "full", "append", "lastmodified").contains(incremental)
    }

    def triggerId: String = {
      if (isSqlQueryJob_?) sqlQueryId
      else if (isDataMartJob_?) dataMartId
      else if (isWorkflowJob_?) workflowId
      else id
    }

    def triggerGroup: String = s"trigger_group_$group"

    def nextRunTime: Option[Date] = QuartzScheduler.getNextRunTime(frequency)

    def cronDescription: String = QuartzScheduler.getCronDescription(frequency)
  }

  @Singleton
  class DAO extends CommonDAO[Model] {

    override val mongoCollectionName = "agendaJobs"

    override def toEntity(dbObject: DBObject): Model = {
      val get = Helper.v(dbObject, _: String)
      val toUpperCase = Helper.toUpperCase _

      val jobType: String = {

        def sqlQueryType: String = {
          val sqlQueryDAO = injector.instanceOf[SqlQuery.DAO]
          sqlQueryDAO.findOneById(get("mongoquery_id")).map(_.queryType).getOrElse("")
        }

        def dataMartType: String = {
          val datamartDAO = injector.instanceOf[DataMart.DAO]
          datamartDAO.findOneById(get("datamart_id")).map(_.dataMartType).getOrElse("")
        }

        (toUpperCase(get("dataSource")), toUpperCase(get("dataSink"))) match {
          case ("SPARKWORKFLOW", _) => "WORKFLOW_SPARK"
          case ("SAPHANA", "HIVE") => "SAPHANA_HIVE_SQOOP"
          case ("POSTGRES", "HIVE") => "POSTGRES_HIVE_SQOOP"
          case ("MYSQL", "HIVE") => "MYSQL_HIVE_SQOOP"
          case ("MSSQL", "HIVE") => "MSSQL_HIVE_SQOOP"
          case ("KDB", "HIVE") => "KDB_HIVE_SQOOP"
          case ("SQLQUERY", _) => sqlQueryType
          case ("ODATA", _) => dataMartType
          case _ => ""
        }
      }

      Model(
        id = dbObject("_id").toString,
        sqlQueryId = get("mongoquery_id"),
        dataMartId = get("datamart_id"),
        workflowId = get("spark_workflow_id"),
        jobType = jobType,
        dataSource = toUpperCase(get("dataSource")),
        dataSink = toUpperCase(get("dataSink")),
        status = toUpperCase(get("status")),
        event = toUpperCase(get("event")),
        frequency = get("jobfrequency"),
        sqoopCommandArgs = convertToMap(dbObject, "commandArguments")
      )
    }

    def updateJobStatus(id: String, event: String, status: String = ""): Unit = {
      val newStatus = if (status.nonEmpty) status else Model.EventStatusMappings.getOrElse(event, "UNKNOWN")
      updateFields(id, Map("event" -> "", "status" -> newStatus, "updatedAt" -> new Date))
    }

    def updateSqoopStatus(objectId: String, state: String, error: String = ""): Unit = {
      val map = Map("status" -> state, "error" -> error, "updatedAt" -> new Date)
      updateFields(objectId, map)
    }

    def getScheduledJobs: Set[Model] = findBatch(DBObject("status" -> "SCHEDULED"))

    def getJobsWithOnholdEvent: Set[Model] = findBatch("event" $in Model.EventStatusMappings.keys)

    def updateSqoopJobSummary(id: String, summary: ResultSummary): Unit = {
      updateScheduledJobSummary("_id", id, summary)
    }

    def updateSqlQuerySummary(sqlQueryId: String, summary: ResultSummary): Unit = {
      updateScheduledJobSummary("mongoquery_id", sqlQueryId, summary)
    }

    def updateDataMartSummary(dataMartId: String, summary: ResultSummary): Unit = {
      updateScheduledJobSummary("datamart_id", dataMartId, summary)
    }

    private def updateScheduledJobSummary(
        field: String, value: String, summary: ResultSummary): Unit = {

      findOneDBObjectBy(field, value).foreach { dbObject =>
        val model = toEntity(dbObject)

        if (model.isScheduled_?) {
          val newResult =
            MongoDBObject(
              "start_at" -> Date.from(summary.startAt),
              "end_at" -> Date.from(summary.endAt),
              "time_taken" -> summary.durationInSecond,
              "state" -> summary.state,
              "error" -> summary.error,
              "next_time" -> model.nextRunTime
            )
          val results = Helper.getList(dbObject, "results")
          newResult +=: results

          val update =
            $set(
              "updatedAt" -> Date.from(summary.endAt),
              "results" -> results.take(ResultSummary.LatestNResults).toList
            )
          collection.update(getDBObject(model.id), update)
        }
      }
    }
  }
}
