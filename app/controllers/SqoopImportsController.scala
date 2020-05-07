
package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{Action, Controller}

import commands.{AbortSqoopCommandBuilder, CommandProcessor}
import models.AgendaJob
import services.kafka.SqoopProducer

/**
  * TODO: THis controller should be deprecated and replaced with [[IngestionJobsController]]
  *
  * @param dao The injected instance of [[models.AgendaJob.DAO]]
  */

@Singleton
class SqoopImportsController @Inject()(dao: AgendaJob.DAO) extends Controller {

  def runOnce = Action(parse.json) { request =>
    val jobId = (request.body \ "agendajob_id").as[String]

    dao.findOneById(jobId) match {
      case Some(job) =>
        SqoopProducer.send(jobId)
        Ok(s"Received Sqoop import job ${job.id}")
      case None => NotFound(s"Job $jobId not found in database")
    }
  }

  def abort = Action(parse.json) { request =>

    val sqoopJobId = (request.body \ "agendajob_id").as[String]

    dao.findOneById(sqoopJobId) match {
      case Some(sqoopJob) =>
        if (sqoopJob.isRunning_?) {
          val cmd = AbortSqoopCommandBuilder(sqoopJob.sourceTableName).build
          val exitCode = CommandProcessor.run(sqoopJobId, "SQOOP_ABORT", cmd)
          if (exitCode == 0) {
            dao.updateSqoopStatus(sqoopJobId, "ABORTED")
            Ok(s"Successfully aborted sqoop job $sqoopJobId")
          } else {
            UnprocessableEntity(s"Failed to abort sqoop job $sqoopJobId")
          }
        } else if (sqoopJob.isQueued_?) {
          dao.updateField(sqoopJobId, "event", "ABORT")
          Ok(s"Successfully aborted sqoop job $sqoopJobId")
        } else {
          Ok(s"Successfully aborted sqoop job $sqoopJobId")
        }
      case None => NotFound(s"Job $sqoopJobId not found in database")
    }
  }
}
