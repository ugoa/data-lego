
package modules

import javax.inject.{Inject, Singleton}

import akka.actor.{ActorSystem, Props}
import services.kafka.StartSignal
import services.kafka.consumers._

/**
  * Start the polling loop to monitor messages from each Kafka topics.
  */
@Singleton
class ConsumerStarter @Inject()(system: ActorSystem) {

  system.actorOf(Props[SqoopToHiveConsumer]) ! StartSignal

  system.actorOf(Props[SparkWriterConsumer]) ! StartSignal

  system.actorOf(Props[OdataToElasticConsumer]) ! StartSignal

  system.actorOf(Props[HiveToCSVConsumser]) ! StartSignal

  system.actorOf(Props[IngestionConsumer]) ! StartSignal

  system.actorOf(Props[SparkWorkflowConsumer]) ! StartSignal
}
