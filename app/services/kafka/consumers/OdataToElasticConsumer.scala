
package services.kafka.consumers

import java.time.Instant

import models.ResultSummary
import models.{AgendaJob, DataMart}
import services.datamart.ODataService
import services.kafka.KafkaConsumer

/**
  * Consumer that download Odata payload from web endpoint, parsing the data as JSON, upload it to HDFS
  * and finally save the data to Elasticsearch.
  */
final class OdataToElasticConsumer extends KafkaConsumer {

  override val topic: String = config.getString("OdataToElasticTopic")
  override val groupId: String = config.getString("OdataToElasticTopicGroup")
  override val zookeeperConnect: String = config.getString("OdataToElasticZookeeperConnect")

  override def consumeMessage(dataMartId: String): Unit =  {
    val dao = injector.instanceOf[DataMart.DAO]

    dao.findOneById(dataMartId) match {
      case Some(dataMart) =>

        dao.updateStatus(dataMart.id, "RUNNING")
        var (startAt, exitCode, state, errorMessage) = (Instant.now, -1, "", "")
        try {
          exitCode = ODataService(dataMart).run()

          if (exitCode == 0) {
            dao.updateStatus(dataMart.id, "COMPLETED")
            logger.info("OData importing completed")
          }
        } catch {
          case ex: Exception =>
            errorMessage = s"Application Exception: ${ex.toString}"
            dao.updateStatus(dataMart.id, "FAILED", s"$errorMessage")
            logger.error(s"OData importing failed", ex)
        }

        val agendaJobDAO = injector.instanceOf[AgendaJob.DAO]
        val summary = ResultSummary(startAt, endAt = Instant.now, state, errorMessage)
        agendaJobDAO.updateDataMartSummary(dataMartId, summary)

      case None => logger.error(s"No data mart found in DB with id: $dataMartId.")
    }
  }

}
