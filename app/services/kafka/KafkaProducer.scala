
package services.kafka

import java.util.{Properties, UUID}

import kafka.message.DefaultCompressionCodec
import kafka.producer.{KeyedMessage, Producer, ProducerConfig => LegacyProducerConfig}
import modules.GlobalContext.injector
import org.apache.kafka.clients.producer
import org.apache.kafka.clients.producer.ProducerRecord

sealed trait LegacyKafkaProducer {
  /**
    * abstract fields, need to be overrided by whom extends this trait
    */
  val brokerList: String
  val topic: String

  val config = injector.instanceOf[play.Configuration]
  val serverURL = config.getString("KafkaBrokerURL")

  /**
    * 1. Use `val` rather than `def` to make sure Producer get instanced only once for each type.
    * 2. Use `lazy` because the brokerlist needs to be provided by the whom extends the trait.
    */
  lazy private val legacyInstance = {
    val props = new Properties()

    props.put("metadata.broker.list", brokerList)
    props.put("compression.codec", DefaultCompressionCodec.codec.toString)
    props.put("producer.type", "sync")
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("client.id", UUID.randomUUID().toString)

    new Producer[String, String](new LegacyProducerConfig(props))
  }

  def send(message: String): Unit = send(Seq(message))

  def send(messages: Seq[String]): Unit = {
    val logger = play.api.Logger
    val producerName = this.getClass.getSimpleName.dropRight(1) // remove $ sign in object name
    try {
      val queueMessages = messages.map { new KeyedMessage[String, String](topic, _) }
      legacyInstance.send(queueMessages: _*)
      logger.info(s"Sent message `${messages.mkString(" ")}` to topic $topic by $producerName")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to send message `${messages.mkString(" ")}` to topic $topic by $producerName")
        ex.printStackTrace()
    }
  }
}

sealed trait NewKafkaProducer {
  /**
    * abstract fields, need to be overrided by whom extends this trait
    */
  val brokerList: String
  val topic: String

  val config = injector.instanceOf[play.Configuration]
  val serverURL = config.getString("KafkaBrokerURL")

  /**
    * 1. Use `val` rather than `def` to make sure Producer get instanced only once for each type.
    * 2. Use `lazy` because the brokerlist needs to be provided by the whom extends the trait.
    */
  lazy private val instance = {
    val props = new Properties()

    props.put("acks", "all")
    props.put("bootstrap.servers", serverURL)
    props.put("retries", "5")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("client.id", UUID.randomUUID().toString)

    new producer.KafkaProducer[String, String](props)
  }

  def send(message: String): Unit = {
    val logger = play.api.Logger
    val producerName = this.getClass.getSimpleName.dropRight(1) // remove $ sign in object name
    try {
      val record = new ProducerRecord[String, String](topic, "scheduler", message)
      instance.send(record)
      logger.info(s"Sent message `$message` to topic $topic by $producerName. Server: $serverURL")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to send message `$message` to topic $topic by $producerName", ex)
    }
  }
}

sealed trait KafkaProducer extends NewKafkaProducer

object SqoopProducer extends KafkaProducer {

  override val brokerList: String = config.getString("consumersqoop")
  override val topic: String = config.getString("topicSqoop")
}

object SparkWriterProducer extends KafkaProducer {

  override val brokerList: String = config.getString("SparkElasticsearchConsumerZK")
  override val topic: String = config.getString("SparkElasticsearchTopic")
}

object OdataToElasticProducer extends KafkaProducer {

  override val brokerList: String = config.getString("OdataToElasticBrokerList")
  override val topic: String = config.getString("OdataToElasticTopic")
}

object HiveToCSVProducer extends KafkaProducer {

  override val topic: String = config.getString("kafka.hiveToCSV.topic")
  override val brokerList: String = config.getString("kafka.hiveToCSV.brokerList")
}

object IngestionProducer extends KafkaProducer {

  override val topic: String = config.getString("kafka.ingestion.topic")
  override val brokerList: String = config.getString("kafka.ingestion.brokerList")
}

object SparkWorkflowProducer extends KafkaProducer {

  override val topic: String = config.getString("kafka.sparkWorkflow.topic")
  override val brokerList: String = config.getString("kafka.sparkWorkflow.brokerList")
}
