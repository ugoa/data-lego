
package services.kafka.consumers

import java.time.Instant

import play.api.libs.json.JsValue

import models.SparkWorkflow
import services.kafka.KafkaConsumer
import services.spark.workflow.WorkflowService

/**
  * Consumer that runs a workflow
  */
final class SparkWorkflowConsumer extends KafkaConsumer {

  override val topic: String = config.getString("kafka.sparkWorkflow.topic")
  override val groupId: String = config.getString("kafka.sparkWorkflow.group")
  override val zookeeperConnect: String = config.getString("kafka.sparkWorkflow.zkConnect")

  override def consumeMessage(sparkWorkflowId: String): Unit = {

    var sessionId: Int = -1
    val workflowService = injector.instanceOf[WorkflowService]
    val workflowDAO = injector.instanceOf[SparkWorkflow.DAO]

    var (startAt, state, log) = (Instant.now, "", "")
    try {

      val workflow = workflowDAO.findOneById(sparkWorkflowId).getOrElse(
        throw new Exception(s"Spark Workflow not found in DB with id `$sparkWorkflowId`")
      )

      val step1Resp: JsValue = workflowService.createSession()

      state = (step1Resp \ "state").as[String]
      sessionId = (step1Resp \ "id").as[Int]

      // Step 1: create session
      var isStarted = false
      while (!isStarted) {
        Thread.sleep(2000)

        val stateResp: JsValue = workflowService.getSessionStatus(sessionId)
        state = (stateResp \ "state").as[String]

        if (state == "idle") isStarted = true
        else if (state == "starting") isStarted = false
        else throw new Exception(s"Invalid state `$state` during livy session creation.")
      }

      // Step 2: Post code statement.
      // state now is 'idle'
      val code = workflowService.LineageTrackingSnippet + "\n" + workflow.codeSnippets
      workflowService.createStatement(sessionId, code)
      var isCompleted = false
      var stateResp: JsValue = null
      while (!isCompleted) {
        Thread.sleep(2000)

        stateResp = workflowService.getStatementStatus(sessionId, 0)
        state = (stateResp \ "state").as[String]

        if (state == "available") isCompleted = true
        else if (state == "running" || state == "waiting") isCompleted = false
        else throw new Exception(s"Invalid state `$state` during running statement")
      }

      val outputStatus = (stateResp \ "output" \ "status").as[String]
      if (outputStatus == "error") {
        state = "FAILED"
        log = (stateResp \ "output" \ "ename").as[String] + ": " + (stateResp \ "output" \ "evalue").as[String]
      } else {
        state = "COMPLETED"
        log = ""
      }
    } catch {
      case ex: Exception =>
        state = "FAILED"
        log = ex.toString
        logger.error(ex.toString)
    } finally {
      workflowDAO.createTransaction(sparkWorkflowId, startAt, state, log)
      if (sessionId != -1) workflowService.deleteSession(sessionId)
    }
  }
}
