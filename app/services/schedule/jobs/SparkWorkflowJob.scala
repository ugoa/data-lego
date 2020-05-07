
package services.schedule.jobs

import org.quartz.{Job, JobExecutionContext}

import services.kafka.SparkWorkflowProducer

class SparkWorkflowJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    SparkWorkflowProducer.send(context.getTrigger.getKey.getName)
  }
}
