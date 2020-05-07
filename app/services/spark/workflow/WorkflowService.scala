
package services.spark.workflow

import javax.inject.{Inject, Singleton}

import play.api.libs.json.JsValue

import models.SparkWorkflow
import services.spark.livy.LivyRESTService

@Singleton
class WorkflowService @Inject()(dao: SparkWorkflow.DAO, val livy: LivyRESTService) {

  val LineageTrackingSnippet: String =
    s"""
       |import za.co.absa.spline.core.SparkLineageInitializer._
       |spark.enableLineageTracking()
       |""".stripMargin

  def createSession(): JsValue = livy.createSession()

  def setSession(id: Int): Int = livy.setSession(id)

  def deleteSession(id: Int): String = livy.deleteSession(id)

  def getSessionStatus(sessionId: Int): JsValue = livy.getSessionStatus(sessionId)

  def createStatement(
      sessionId: Int, code: String, codeType: String = LivyRESTService.CodeType.Spark): JsValue = {

    livy.postStatement(sessionId, code, codeType)
  }

  def createStatement(workflowId: String, sessionId: Int, orderId: Int): JsValue = {

    dao.findOneById(workflowId) match {
      case Some(workflow) =>
        if (orderId < 1 || orderId > workflow.nodeCount) {
          throw new Exception(s"Order ID $orderId is out of range 1 - ${workflow.nodeCount}")
        }
        val node = workflow.sortedNodes(orderId - 1)

        livy.postStatement(sessionId, node.codeSnippet, node.codeType)
      case None =>
        throw new Exception(s"Spark workflow NOT found in DB with id `$workflowId`")
    }
  }

  def enableLineageTracking(sessionId: Int): JsValue = {
    livy.postStatement(sessionId, LineageTrackingSnippet)
  }

  def getStatementStatus(sessionId: Int, statementId: Int): JsValue = {
    livy.getStatementStatus(sessionId, statementId)
  }

  def getDataFramePreview(
      workflowId: String, sessionId: Int, nodeOrderId: Int, outputId: Int): JsValue = {

    dao.findOneById(workflowId) match {
      case Some(workflow) =>
        val node = workflow.sortedNodes(nodeOrderId - 1)

        val error = s"NO DataFrame output from node ${node.name} as #$nodeOrderId in workflow $workflowId"
        val dfVal = node.getDataFrameVal(outputId).getOrElse(throw new Exception(error))
        livy.getDFPreview(sessionId, dfVal)
      case None =>
        throw new Exception(s"Spark workflow NOT found in DB with id `$workflowId`")
    }
  }
}

