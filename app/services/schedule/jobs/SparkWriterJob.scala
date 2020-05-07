
package services.schedule.jobs

import org.quartz.{Job, JobExecutionContext}
import services.kafka.SparkWriterProducer

class SparkWriterJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    SparkWriterProducer.send(context.getTrigger.getKey.getName)
  }
}
