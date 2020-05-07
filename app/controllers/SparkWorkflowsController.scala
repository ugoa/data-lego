
package controllers

import javax.inject.{Inject, Singleton}

import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import services.spark.workflow.WorkflowService

/**
  * Handle requests of livy session creating/deleting, statement execution and data preview etc.
  *
  * @param workflowService
  */
@Singleton
class SparkWorkflowsController @Inject()(workflowService: WorkflowService) extends Controller {

  /**
    * Create a Livy session.
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]]
    */
  def createSession(workflowId: String) = Action {
    try {
      val newSessionJson = workflowService.createSession()
      Created(newSessionJson)
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to create Session. ${ex.toString}")
    }
  }

  /**
    * Get Livy session status.
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]]
    * @param sessionId The id of created Livy session.
    */
  def getSession(workflowId: String, sessionId: String) = Action {

    try {
      val sessionStatus = workflowService.getSessionStatus(sessionId.toInt)
      Ok(sessionStatus)
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to get status for session $sessionId. ${ex.toString}")
    }
  }

  /**
    * Delete a Livy session.
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]]
    * @param sessionId The id of Livy session to be created.
    */

  def deleteSession(workflowId: String, sessionId: String) = Action {
    try {
      val resp = workflowService.deleteSession(sessionId.toInt)
      Ok(Json.parse(resp))
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to delete session. ${ex.toString}")
    }
  }

  /**
    * Execute a piece of statement code within session.
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]].
    * @param sessionId The id of Livy session where the code to be executed.
    */

  def createStatement(workflowId: String, sessionId: String) = Action(parse.json) { request =>
    try {
      val orderId = (request.body \ "order_id").as[Int]
      val respJson = workflowService.createStatement(workflowId, sessionId.toInt, orderId)

      Ok(respJson)
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to create statement. ${ex.getMessage}")
    }
  }

  /**
    * Enable Spark lineage for the session.
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]].
    * @param sessionId The id of the session.
    */
  def enableLineage(workflowId: String, sessionId: String) = Action(parse.json) { request =>
    try {
      val respJson = workflowService.enableLineageTracking(sessionId.toInt)

      Ok(respJson)
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to create statement. ${ex.getMessage}")
    }
  }

  /**
    * Get statement execution status.
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]].
    * @param sessionId The id of the session
    * @param statementId The statement id to be checked. It's generated and returned when a new
    *                    statement being submitted to execute.
    */
  def getStatement(workflowId: String, sessionId: String, statementId: String) = Action {
    try {
      val statusJson = workflowService.getStatementStatus(sessionId.toInt, statementId.toInt)
      Ok(statusJson)
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to get status for statement $statementId. ${ex.toString}")
    }
  }

  /**
    * Get preview of an node output of which type is a DataFrame
    *
    * @param workflowId The value of [[models.SparkWorkflow.Model.id]].
    * @param sessionId The id of the session
    * @param orderId the orderId of the node which produces the Dataframe output.
    *                See: [[models.SparkWorkflow.Node.orderId]]
    * @param outputId the sequence Id of the DataFrame output within the node.
    *                 See: [[models.SparkWorkflow.Node.outputs]]
    */
  def getDFPreview(workflowId: String, sessionId: Int, orderId: Int, outputId: Int) = Action {

    try {
      val preview = workflowService.getDataFramePreview(workflowId, sessionId, orderId, outputId)
      Ok(preview)
    } catch {
      case ex: Exception =>
        Logger.error(ex.toString)
        UnprocessableEntity(s"Failed to get preview for. ${ex.toString}")
    }
  }
}


