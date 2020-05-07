
package services.schedule.jobs

import org.quartz.{Job, JobExecutionContext}
import services.kafka.OdataToElasticProducer

class ODataToElasticJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    OdataToElasticProducer.send(context.getTrigger.getKey.getName)
  }
}

