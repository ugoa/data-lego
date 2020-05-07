
package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{Action, Controller}

import commands.{AbortSparkSubmitCommandBuilder, AbortSqoopCommandBuilder, CommandProcessor}
import models.IngestionJob
import services.kafka.IngestionProducer

@Singleton
class IngestionJobsController @Inject()(dao: IngestionJob.DAO) extends Controller {

  def run(ingestionId: String) = Action(parse.json) { request =>

    dao.findOneById(ingestionId) match {
      case Some(job) =>
        IngestionProducer.send(ingestionId)
        Ok(s"Received ingestion job $ingestionId")
      case None => NotFound(s"Job $ingestionId not found in database")
    }
  }

  def abort(ingestionId: String) = Action(parse.json) { request =>

    dao.findOneById(ingestionId) match {
      case Some(ingestionJob) =>

        if (ingestionJob.isRunning_?) {

          val cmd = if (ingestionJob.bySpark) {
            AbortSparkSubmitCommandBuilder(ingestionJob.hiveTable, "").build
          } else {
            AbortSqoopCommandBuilder(ingestionJob.hiveTable).build
          }

          val exitCode = CommandProcessor.run(ingestionId, "INGESTIONJOB_ABORT", cmd)
          if (exitCode == 0) {
            dao.updateIngestionJobState(ingestionId, "ABORTED")
            Ok(s"Successfully aborted ingestion job $ingestionId")
          } else {
            UnprocessableEntity(s"Failed to abort ingestion job $ingestionId")
          }
        } else if (ingestionJob.isQueued_?) {
          dao.setToBeAborted(ingestionId)
          Ok(s"Successfully aborted ingestion job $ingestionId")
        } else {
          Ok(s"Successfully aborted ingestion job $ingestionId")
        }
      case None => NotFound(s"Job $ingestionId not found in database")
    }
  }
}
