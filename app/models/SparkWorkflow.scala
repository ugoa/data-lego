
package models

import java.lang.reflect.InvocationTargetException
import java.time.Instant
import javax.inject.Singleton

import com.mongodb.casbah.Imports._

import models.Helper.{get, getInt, getList}
import models.sparkworkflow.statement.Statement
import modules.GlobalContext.injector

object SparkWorkflow {

  /**
    * @param parentId
    * @param outputId
    * @param outputType
    * @param paramOverwrites
    *   param key-values that inherit from and overwrite the parent Node. e.g.
    *     "param_overwrites" : {
    *       "maxDepth" : "integer:5,10,20",
    *       "numTrees" : "double:2.0,4.5"
    *     }
    */
  case class ParentOutput(
    parentId: Int,
    outputId: Int,
    outputType: String,
    paramOverwrites: Map[String, String]
  )

  /**
    * @param orderId Execution sequence id
    * @param name Concrete statement name. e.g. 'ReadDataFrame', `Join`, `WriteDataFrame`
    * @param category statement category e.g. `input`, `setoperation`, `output`
    * @param parameters parameters for each statement
    * @param parentOutputs
    *   The info list of the parent nodes, whose outputs are going to be input of current Node
    */
  case class Node(
      orderId: Int,
      name: String,
      category: String,
      parameters: Map[String, String],
      outputs: Vector[String],
      parentOutputs: Vector[ParentOutput]) {

    lazy val statementInstance: Statement = {
      val concreteStatement = s"models.sparkworkflow.statement.$category.$name"
      try {
        val klass = Class.forName(concreteStatement)
        val constructor = klass.getConstructors()(0)
        constructor.newInstance(this).asInstanceOf[Statement]
      } catch { case ex: InvocationTargetException => throw ex.getTargetException }
    }

    lazy val codeSnippet: String = statementInstance.snippet

    lazy val codeType: String = statementInstance.codeType

    def getDataFrameVal(outputId: Int): Option[String] = {
      statementInstance.getDataFrameOutput(outputId)
    }
  }

  case class Model(id: String, name: String, userId: String, nodes: Vector[Node]) {

    lazy val sortedNodes: Vector[Node] = nodes.sortBy(_.orderId)

    lazy val nodeCount: Int = sortedNodes.length

    lazy val codeSnippets: String = sortedNodes.map(_.codeSnippet).mkString("\n")

    def codeSnippets(upTo: Int): String = {
      val n = if (upTo > 0) upTo else nodeCount
      sortedNodes.take(n).map(_.codeSnippet).mkString("\n")
    }
  }

  @Singleton
  class DAO extends CommonDAO[Model] {

    override val mongoCollectionName: String = "spark_workflows"

    override def toEntity(workflowObject: DBObject): Model = {

      def nodes: Vector[Node] = {
        val nodeList = getList(workflowObject, "nodes").toVector
        nodeList.map { element =>
          val nodeObject = element.asInstanceOf[DBObject]

          val parentOutputs: Vector[ParentOutput] = {
            val parentParamArray = getList(nodeObject, "parent_outputs").toVector
            parentParamArray.map { param =>
              val paramObj = param.asInstanceOf[DBObject]
              ParentOutput(
                parentId = getInt(paramObj, "parent_id"),
                outputId = getInt(paramObj, "output_id"),
                outputType = get(paramObj, "output_type"),
                paramOverwrites = convertToMap(paramObj, "param_overwrites")
              )
            }
          }

          Node(
            orderId = getInt(nodeObject, "order_id"),
            name = get(nodeObject, "node_name"),
            category = get(nodeObject, "category").toLowerCase.replaceAll("_", "."),
            parameters = convertToMap(nodeObject, "parameters"),
            outputs = getList(nodeObject, "outputs").map(_.asInstanceOf[String]).toVector,
            parentOutputs = parentOutputs
          )
        }
      }

      Model(
        id = workflowObject("_id").toString,
        name = get(workflowObject, "name"),
        userId = get(workflowObject, "user_id"),
        nodes = nodes
      )
    }


    def createTransaction(workflowId: String, startAt: Instant, state: String, log: String): Unit = {

      findOneById(workflowId).foreach { workflow =>
        val transaction = JobTransaction.Model(
          jobType = "SPARKWORKFLOW",
          jobId = workflowId,
          userId = workflow.userId,
          startAt = startAt,
          endAt = Instant.now,
          state = state,
          log = log,
          triggerBy = "SYSTEM",
          createdAt = Instant.now,
          updatedAt = Instant.now
        )

        val jobTransactionDAO = injector.instanceOf[JobTransaction.DAO]
        jobTransactionDAO.createJobTransaction(transaction)
      }
    }
  }
}
