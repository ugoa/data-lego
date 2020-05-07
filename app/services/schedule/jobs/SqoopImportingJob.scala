
package services.schedule.jobs

import org.quartz.{Job, JobExecutionContext}
import services.kafka.SqoopProducer

class SqoopImportingJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    SqoopProducer.send(context.getTrigger.getKey.getName)
  }
}
