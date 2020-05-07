
package modules

import javax.inject.{Inject, Singleton}

import models.AgendaJob
import services.schedule.AgendaJobService

/**
  * Module that
  *  1) picks up the left events of the agendajob and execution.
  *  1) Schedule the job with status 'SCHEDULED'
  */
@Singleton
class JobStarter @Inject()(dao: AgendaJob.DAO, agendaJobService: AgendaJobService) {

  dao.getScheduledJobs.foreach(agendaJobService.schedule)

  dao.getJobsWithOnholdEvent.foreach(agendaJobService.dispatch)
}
