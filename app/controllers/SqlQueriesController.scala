
package controllers

import javax.inject.{Inject, Singleton}

import commands.{AbortSparkSubmitCommandBuilder, CommandProcessor}
import play.api.mvc.{Action, Controller}
import services.kafka._
import models.SqlQuery

@Singleton
class SqlQueriesController @Inject() (dao: SqlQuery.DAO) extends Controller {

  def stats = Action(parse.json) { implicit request =>
    val statsId = (request.body \ "statistics_id").as[String]
    val reqTransformer_id = (request.body \ "transformer_id").as[String]

    SparkWriterProducer.send(s"$statsId###$reqTransformer_id")
    Ok("received stats command")
  }

  def queryRunOnce = Action(parse.json) { implicit request =>
    val sqlQueryId = (request.body \ "mongoquery_id").as[String]

    dao.findOneById(sqlQueryId) match {
      case Some(sqlQuery) =>
        if (sqlQuery.queryType == "SQLQUERY_HIVE_DRILL" || sqlQuery.queryType == "SQLQUERY_ELASTIC_DRILL") {
          UnprocessableEntity(s"Drill API no longer supported")
        } else {
          SparkWriterProducer.send(sqlQueryId)
          Ok("received command")
        }
      case None => NotFound(s"Job $sqlQueryId not found in database")
    }
  }

  def abort = Action(parse.json) { request =>
    val sqlQueryId = (request.body \ "mongoquery_id").as[String]

    dao.findOneById(sqlQueryId) match {
      case Some(query) =>
        if (query.isRunning_?) {
          val cmd = AbortSparkSubmitCommandBuilder(query.destinationTable, query.clusterNode).build
          val exitcode = CommandProcessor.run(sqlQueryId, "SPARK_ABORT", cmd)
          if (exitcode == 0) {
            dao.updateStatus(sqlQueryId, "ABORTED")
            Ok(s"Successfully aborted spark submit job $sqlQueryId")
          } else {
            UnprocessableEntity(s"Failed to abort spark submit job $sqlQueryId")
          }
        } else if (query.isQueued_?) {
          dao.updateStatus(sqlQueryId, "ABORTING")
          Ok(s"Successfully cancelled spark submit job $sqlQueryId")
        } else {
          Ok(s"Successfully aborted spark submit job $sqlQueryId")
        }
      case None => NotFound(s"Spark submit job $sqlQueryId not found in database")
    }
  }
}
