
package controllers

import java.io.File
import javax.inject.{Inject, Singleton}

import play.api.mvc.{Action, Controller}
import akka.actor.ActorSystem
import models.DataMart
import services.datamart.CSVUploadActor
import services.datamart.CSVUploadActor._
import services.kafka.OdataToElasticProducer
import utils.Helper

@Singleton
class UploadsController @Inject()(actorSystem: ActorSystem, dao: DataMart.DAO) extends Controller {

  def upload(dataMartId: String) = Action(parse.multipartFormData) { implicit request =>

    val tmpCSVPath = s"${Helper.AppRootPath}/tmp/uploads/$dataMartId.csv"
    val file = new File(tmpCSVPath)
    file.getParentFile.mkdirs

    request.body.file("file").map(_.ref.moveTo(file))

    dao.findOneById(dataMartId) match {
      case Some(dataMart) =>
        val csvUploadActor = actorSystem.actorOf(CSVUploadActor.props)
        csvUploadActor ! UploadParams(dataMart, tmpCSVPath)
        Ok(s"File received, importing..")
      case None =>
        NotFound(s"Datamart $dataMartId not found in database")
    }
  }

  def oDataImport(dataMartId: String) = Action(parse.json) { implicit request =>
    dao.findOneById(dataMartId) match {
      case Some(dataMart) =>
        if (dataMart.dataSource == "ODATA") {
          OdataToElasticProducer.send(dataMartId)
          Ok("Odata importing...")
        } else {
          UnprocessableEntity("Invalid ODATA datasource")
        }
      case None => NotFound(s"datamart $dataMartId not found in database")
    }
  }
}
