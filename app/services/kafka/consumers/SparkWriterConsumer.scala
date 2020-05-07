
package services.kafka.consumers

import akka.actor.{ActorRef, ActorSystem, Props}

import models.SqlQuery
import services.kafka.KafkaConsumer
import services.kafka.consumers.SparkWriterActor.{RunQuery, RunStats}
import services.kafka.consumers.SparkWriterConsumer.NodeActor
import utils.Helper.getNodeList

/**
  * Main consumer that runs all Spark SQL jobs.
  */
final class SparkWriterConsumer extends KafkaConsumer {

  override val topic: String = config.getString("SparkElasticsearchTopic")
  override val groupId: String = config.getString("SparkElasticsearchGroupID")
  override val zookeeperConnect: String = config.getString("SparkElasticsearchProducerZK")

  /**
    * For each of the cluster nodes there is one actor assigned to it. So the queries will be able
    * to run on diffrent nodes simultaneously.
    * This design is mainly for EDP which runs the job in stand-alone mode. The purpose is to bring
    * limited concurrency to EDP cluster.
    */
  private lazy val nodeActorMapppings: Vector[NodeActor] = {
    val system = injector.instanceOf[ActorSystem]
    getNodeList.map { node => NodeActor(node.nodeName, system.actorOf(Props[SparkWriterActor])) }
  }

  override def consumeMessage(message: String): Unit = message.split("###") match {

    case Array(sqlQueryId) =>

      val sqlQueryDAO: SqlQuery.DAO = injector.instanceOf[SqlQuery.DAO]
      sqlQueryDAO.findOneById(sqlQueryId) match {
        case Some(sqlQuery) =>
          nodeActorMapppings.find(_.nodeName == sqlQuery.clusterNode) match {
            case Some(nodeActor) =>
              nodeActor.actorRef ! RunQuery(sqlQueryId)
            case None =>
              logger.error(s"Unknown node key `${sqlQuery.clusterNode}` to run query")
          }
        case None => logger.error(s"No query found in DB with id: `$sqlQueryId`")
      }
    case Array(statisticId, transformerId) =>
      val defaultActor: ActorRef = nodeActorMapppings.head.actorRef
      defaultActor ! RunStats(statisticId, transformerId)
    case _ =>
      throw new Exception("Not Supported Spark process.")
  }
}

object SparkWriterConsumer {

  case class NodeActor(nodeName: String, actorRef: ActorRef)
}
