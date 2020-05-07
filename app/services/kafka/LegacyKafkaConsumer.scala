
/**
  * Created by david on 25/8/17.
  */
package services.kafka

import java.time.{Instant, Duration}
import java.util.Properties

import akka.actor.Actor
import kafka.consumer._
import kafka.serializer.DefaultDecoder

trait LegacyKafkaConsumer extends Actor {

  val zookeeperConnect: String
  val groupId: String
  val topic: String
  def consumeMessage(message: String): Unit

  val injector = modules.GlobalContext.injector
  val logger = play.api.Logger
  val config = injector.instanceOf[play.Configuration]

  lazy private val connector = {
    val props = new Properties()

    props.put("group.id", groupId)
    props.put("zookeeper.connect", zookeeperConnect)
    props.put("auto.offset.reset", "smallest")
    props.put("consumer.timeout.ms", "-1")
    props.put("auto.commit.interval.ms", "10000")

    Consumer.create(new ConsumerConfig(props))
  }

  lazy private val iterator = {
    val topicFilter = Whitelist(topic)
    val streams =
      connector
        .createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder())
        .head

    streams.iterator
  }

  private def read(): Option[String] = {
    try {
      if (iterator.hasNext()) Some(new String(iterator.next().message()))
      else None
    } catch {
      case ex: Exception =>
        logger.error(s"Getting error ${ex.getMessage} when reading message ", ex)
        None
    }
  }

  def close(): Unit = connector.shutdown()

  override def receive: Receive = {
    case StartSignal =>
      val consumerName = this.getClass.getSimpleName
      logger.info(s"Listening to topic $topic by $consumerName")
      while (true) {
        read() match {
          case Some(message) =>
            logger.info(s"Received message `$message` on topic $topic by $consumerName.")
            val startTime = Instant.now

            try {
              consumeMessage(message)
            } catch {
              case ex: Exception => logger.error(s"$consumerName Error.", ex)
            }

            val endTime = Instant.now
            val duration = Duration.between(startTime, endTime).toMillis / 1000.0
            logger.info(s"TIME COST: $duration seconds. Listening to topic $topic by $consumerName")
          case None =>
        }
        Thread.sleep(2 * 1000)
      }
    case _ => None
  }
}

