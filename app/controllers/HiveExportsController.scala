
package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{Action, Controller}

import services.kafka.HiveToCSVProducer

import models.HiveExport

@Singleton
class HiveExportsController @Inject()(dao: HiveExport.DAO) extends Controller {

  def csvExport(exportId: String) = Action {
    dao.findOneById(exportId) match {
      case Some(hiveExport) =>
        HiveToCSVProducer.send(hiveExport.id)
        Ok(s"Received Hive table export task $exportId")
      case None =>
        NotFound(s"hive export task $exportId not found in database")
    }
  }
}
