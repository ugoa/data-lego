
package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{Action, Controller}
import models.AgendaJob
import services.schedule.AgendaJobService

/**
  * Handle requests of job scheduling/re-scheduling/pause/cancelling
  *
  * @param dao
  * @param agendaJobService
  */
@Singleton
class AgendaJobsController @Inject()(
    dao: AgendaJob.DAO, agendaJobService: AgendaJobService) extends Controller {

  def dispatch(action: String) = Action(parse.json) { implicit request =>

    val jobId = (request.body \ "agendajob_id").as[String]

    dao.findOneById(jobId) match {
      case Some(job) =>
        if (job.eventValid_?) {
          agendaJobService.dispatch(job)
          Ok(s"Received ${job.event} event for job ${job.id}")
        } else {
          UnprocessableEntity(s"Invalid event for job $jobId, failed to run `${job.event}`.")
        }
      case None => NotFound(s"Job $jobId not found in database")
    }
  }
}
