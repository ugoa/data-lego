
package services.schedule

import javax.inject.{Inject, Singleton}

import play.api.Logger
import models.AgendaJob
import services.kafka.SqoopProducer

/**
  * AgendaJob service that handle all job scheduling requests.
  */
@Singleton
class AgendaJobService @Inject()(dao: AgendaJob.DAO) {

  private val schedulerService = services.schedule.QuartzScheduler

  def dispatch(job: AgendaJob.Model): Unit = {
    if (job.eventValid_?) {
      job.event match {
        case "IMPORT" => SqoopProducer.send(job.id)
        case "SCHEDULE" => schedule(job)
        case "PAUSE" =>
          schedulerService.pause(job)
          dao.updateJobStatus(job.id, "PAUSE")
        case "CANCEL" =>
          schedulerService.cancel(job)
          dao.updateJobStatus(job.id, "CANCEL")
      }
    } else {
      Logger.error(s"Job ${job.id} state is not valid, failed to run ${job.event}.")
    }
  }

  def schedule(job: AgendaJob.Model): Unit = {
    if (schedulerService.jobExisted_?(job.id, job.group)) schedulerService.reschecule(job)
    else if (schedulerService.jobPaused_?(job)) schedulerService.resume(job)
    else schedulerService.schedule(job)
    dao.updateJobStatus(job.id, "SCHEDULE")
    dao.updateField(job.id, "frequency_description", job.cronDescription)
  }

  def cancelJob(jobId: String): Unit = {
    dao.findOneById(jobId).foreach { job =>
      schedulerService.cancel(job)
      dao.updateJobStatus(job.id, "CANCEL")
    }
  }
}
