
package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{Action, Controller}
import models.Group
import connectors.HiveConnector

/**
  * Handle requests related groups.
  *
  * @param dao
  * @param hiveConn
  */
@Singleton
class GroupsController @Inject()(dao: Group.DAO, hiveConn: HiveConnector) extends Controller {

  def createSchema() = Action(parse.json) { implicit request =>

    val group_id = (request.body \ "group_id").as[String]
    dao.findOneById(group_id) match {
      case Some(group) =>
        try {
          hiveConn.createSchema(group.hiveSchema)
          Created(s"Hive schema ${group.hiveSchema} created")
        } catch {
          case ex: Exception =>
            UnprocessableEntity(s"Unable to create hive schema, ${ex.toString}")
        }
      case None => NotFound(s"group $group_id not find in DB")
    }
  }
}
