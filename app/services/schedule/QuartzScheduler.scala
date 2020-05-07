
package services.schedule

import java.util.{Date, TimeZone}

import scala.collection.JavaConverters._

import models.AgendaJob
import org.quartz.CronScheduleBuilder.cronSchedule
import org.quartz.JobBuilder.newJob
import org.quartz.JobKey.jobKey
import org.quartz.TriggerBuilder.newTrigger
import org.quartz.TriggerKey.triggerKey
import org.quartz.impl.StdSchedulerFactory
import org.quartz.{CronExpression, Job, Scheduler, Trigger}
import services.schedule.jobs._

/**
  * Wrapper of Quartz library.
  * @see <a href="http://www.quartz-scheduler.org/">
  */
object QuartzScheduler {

  private val scheduler: Scheduler = StdSchedulerFactory.getDefaultScheduler
  scheduler.start()

  private def buildTrigger(job: AgendaJob.Model): Trigger = {
    newTrigger()
      .withIdentity(job.triggerId, job.triggerGroup)
      .withSchedule(
        cronSchedule(job.frequency)
        .inTimeZone(TimeZone.getTimeZone("UTC"))
      )
      .startNow()
      .build()
  }

  private def getQuartzJobType(agendaJobType: String): Class[_ <: Job] = agendaJobType match {

    case "SAPHANA_HIVE_SQOOP" | "POSTGRES_HIVE_SQOOP" | "KDB_HIVE_SQOOP" | "MYSQL_HIVE_SQOOP" |
         "MSSQL_HIVE_SQOOP" | "REDSHIFT_HIVE_SQOOP" | "SNOWFLAKE_HIVE_SQOOP" |
         "SALESFORCE_HIVE_SQOOP" => classOf[SqoopImportingJob]
    case "SAPHANA_HIVE_SPARK" | "POSTGRES_HIVE_SPARK" | "ORACLE_HIVE_SPARK" | "MYSQL_HIVE_SPARK" |
         "MSSQL_HIVE_SPARK" | "REDSHIFT_HIVE_SPARK" | "SNOWFLAKE_HIVE_SPARK" |
         "SALESFORCE_HIVE_SPARK" => classOf[IngestionQuartzJob]
    case "SQLQUERY_ELASTIC_SPARK" | "SQLQUERY_HIVE_SPARK" => classOf[SparkWriterJob]
    case "DATAMART_ODATA_ELASTIC" => classOf[ODataToElasticJob]
    case "WORKFLOW_SPARK" => classOf[SparkWorkflowJob]
    case _ => throw new Exception(s"Scheduling failed due to invalid job type `$agendaJobType`")
  }

  def jobExisted_?(jobId: String, jobGroup: String): Boolean = {
    scheduler.checkExists(jobKey(jobId, jobGroup))
  }

  def jobPaused_?(job: AgendaJob.Model): Boolean = {
    val tKey = triggerKey(job.triggerId, job.triggerGroup)
    scheduler.getTriggerState(tKey) == Trigger.TriggerState.PAUSED
  }

  def isJobExcuting_?(jobId: String, jobGroup: String): Boolean = {
    val allRunningJobs = scheduler.getCurrentlyExecutingJobs.asScala
    allRunningJobs.exists(context => {
      val key = context.getJobDetail.getKey
      key.getName == jobId && key.getGroup == jobGroup
    })
  }

  def schedule(job: AgendaJob.Model): Unit = {
    try {
      val quartzJobType = getQuartzJobType(job.jobType)

      val jobDetail = newJob(quartzJobType).withIdentity(job.id, job.group).build()

      scheduler.scheduleJob(jobDetail, buildTrigger(job))
      play.api.Logger.info(s"Successfully scheduled job: ${job.id}")
    } catch {
      case ex: Exception => play.api.Logger.error(s"Failed to schedule job ${job.id}, ${ex.getMessage}")
    }
  }

  def reschecule(job: AgendaJob.Model): Unit = {
    try {
      val oldTriggerKey = triggerKey(job.triggerId, job.triggerGroup)
      scheduler.rescheduleJob(oldTriggerKey, buildTrigger(job))
      play.api.Logger.info(s"Successfully re-scheduled job: ${job.id}")
    } catch {
      case ex: Exception => play.api.Logger.error(s"Failed to Re-schedule job ${job.id}, ${ex.getMessage}")
    }
  }

  def getNextRunTime(cron: String): Option[Date] = {
    if (CronExpression.isValidExpression(cron)) {
      val cronExpr = new CronExpression(cron)
      Some(cronExpr.getNextValidTimeAfter(new Date))
    } else None
  }

  def getCronDescription(cron: String): String = {
    if (CronExpression.isValidExpression(cron)) {
      val cronExpr = new CronExpression(cron)
      cronExpr.getExpressionSummary
    } else s"Invalid cron expression $cron"
  }

  def cancel(job: AgendaJob.Model): Unit = {
    scheduler.deleteJob(jobKey(job.id, job.group))
    play.api.Logger.info(s"Successfully cancelled job: ${job.id}")
  }

  def pause(job: AgendaJob.Model): Unit = {
    scheduler.pauseJob(jobKey(job.id, job.group))
    play.api.Logger.info(s"Successfully paused job: ${job.id}")
  }

  def resume(job: AgendaJob.Model): Unit = {
    scheduler.resumeJob(jobKey(job.id, job.group))
    play.api.Logger.info(s"Successfully resumed job: ${job.id}")
  }
}
