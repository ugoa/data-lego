
package controllers

import javax.inject.Singleton
import play.api.mvc.{Action, Controller}
import modules.GlobalContext.injector
import models._

/**
  * Handle requests that related to Data sinks.
  */

@Singleton
class DataSinkController extends Controller {

  /**
    * Drop a table in the data sink.
    *
    * @param sink Sink type.
    */
  def dropTable(sink: String) = Action(parse.json) { implicit request =>

    def v(param: String) = (request.body \ param).as[String]

    val (objectId, dao) = sink match {
      case "Query" => (v("mongoquery_id"), injector.instanceOf[SqlQuery.DAO])
      case "DataMart" => (v("datamart_id"), injector.instanceOf[DataMart.DAO])
      case "DataLake" => (v("datalake_id"), injector.instanceOf[HiveTableInfo.DAO])
    }

    dao.dropTable(objectId) match {
      case (true, message) => Ok(message)
      case (false, message) => UnprocessableEntity(message)
    }
  }
}
