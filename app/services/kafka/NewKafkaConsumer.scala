
/**
  * Created by david on 25/8/17.
  */
package services.kafka

import java.time.{Instant, Duration}
import java.util.Properties

import scala.collection.JavaConverters._

import akka.actor.Actor
import org.apache.kafka.clients.consumer

trait NewKafkaConsumer extends Actor {

  val zookeeperConnect: String
  val groupId: String
  val topic: String
  def consumeMessage(message: String): Unit

  val injector = modules.GlobalContext.injector
  val logger = play.api.Logger
  val config = injector.instanceOf[play.Configuration]

  private def createConsumerInstance() = {
    val props = new Properties()
    val serverURL = config.getString("KafkaBrokerURL")
    props.put("bootstrap.servers", serverURL)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val instance = new consumer.KafkaConsumer[String, String](props)
    instance.subscribe(java.util.Arrays.asList(topic))
    instance
  }

  override def receive: Receive = {

    case StartSignal =>
      val consumerName = this.getClass.getSimpleName
      logger.info(s"Listening to topic $topic by $consumerName")

      while (true) {
        val consumerInstance = createConsumerInstance()
        var records = Vector[(String, Long)]()
        try {
          records =
            consumerInstance
              .poll(Long.MaxValue)
              .asScala
              .map { record => (record.value, record.offset) }
              .toVector
        } finally {
          consumerInstance.close()
        }

        records.foreach { case (message, offset) =>
          logger.info(s"Received message `$message` on topic $topic by $consumerName. Offset: $offset")

          val startTime = Instant.now

          try {
            consumeMessage(message)
          } catch {
            case ex: Exception => logger.error(s"$consumerName Error.", ex)
          }

          val endTime = Instant.now
          val duration = Duration.between(startTime, endTime).toMillis / 1000.0
          logger.info(s"TOTAL TIME COST: $duration seconds. Listening to topic $topic by $consumerName")
        }
      }
    case _ => None
  }
}

