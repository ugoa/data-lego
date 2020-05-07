
package services.schedule.jobs

import org.quartz.{Job, JobExecutionContext}
import services.kafka.IngestionProducer

class IngestionQuartzJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    IngestionProducer.send(context.getTrigger.getKey.getName)
  }
}
