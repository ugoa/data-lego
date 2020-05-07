
package services.kafka.consumers

import java.time.{Duration, Instant}

import play.api.Logger

import models.{AgendaJob, SqlQuery}
import models.ResultSummary
import modules.GlobalContext.injector

/**
  * The trait for general process of running Spark SQL jobs.
  */
trait SqlQueryRunner {

  protected val sqlQueryDAO: SqlQuery.DAO = injector.instanceOf[SqlQuery.DAO]

  /**
    * query running process.
    * @param sqlQueryId
    * @return processFn function passed in to handle each scenarios
    *         exit code. 0 - success, others - failed. Mainly for handling spark jobs
    */
  def run(sqlQueryId: String, processFn: SqlQuery.Model => Int): Unit = {

    sqlQueryDAO.findOneById(sqlQueryId) match {
      case Some(sqlQuery) =>

        var (startAt, exitCode, state, errorMessage) = (Instant.now, -1, "", "")
        try {
          if (sqlQuery.toBeCancelled_?) {
            sqlQueryDAO.updateStatus(sqlQueryId, "ABORTED")
            return
          }

          sqlQueryDAO.updateStatus(sqlQueryId, "RUNNING")
          sqlQueryDAO.updateQueryResultTable(sqlQueryId)

          exitCode = processFn(sqlQuery)

          val duration = Duration.between(startAt, Instant.now).toMillis / 1000.0
          if (exitCode == 0) {
            state = "COMPLETED"
            Logger.info(s"Query job COMPLETED. TIME COST: $duration seconds")
            sqlQueryDAO.updateStatus(sqlQueryId, "COMPLETED")
          } else {
            state = "FAILED"
            Logger.info(s"Query job FAILED. TIME COST: $duration seconds")
          }
          // No need to update status to FAILED if exitCode is not 0,
          // because it'd already being done in the spark job apps,
          // But need to update the 'state' in case it's scheduled query
        } catch {
          case ex: Exception =>
            state = "FAILED"
            errorMessage = s"Application Exception: ${ex.toString}"
            Logger.error(errorMessage, ex)
            sqlQueryDAO.updateStatus(sqlQueryId, "FAILED", errorMessage)
        }

        val agendaJobDAO = injector.instanceOf[AgendaJob.DAO]
        val summary = ResultSummary(startAt, endAt = Instant.now, state, errorMessage)
        agendaJobDAO.updateSqlQuerySummary(sqlQuery.id, summary)

      case None => Logger.error(s"No Sql query found in DB with id: $sqlQueryId.")
    }
  }
}
