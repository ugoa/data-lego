
package models

import play.api.Logger
import org.bson.types.ObjectId
import com.mongodb.casbah.Imports._
import connectors._
import modules.GlobalContext.injector
import services.schedule.AgendaJobService

trait Droppable {

  val dataSinkField: String
  val tableField: String
  val hiveSchemaField: String
  def collection: MongoCollection

  def dropTable(objectId: String): (Boolean, String) = {

    collection.findOneByID(new ObjectId(objectId)) match {
      case Some(dbObject) =>
        try {
          val get = Helper.get(dbObject, _: String)

          val dataSink = Helper.toUpperCase(get(dataSinkField))
          val tableName = get(tableField)
          dataSink match {
            case "HIVE" => dropHiveRelatedTable(tableName, get(hiveSchemaField))
            case "ELASTIC" =>
              injector.instanceOf[ElasticSearchConnector].deleteIndexIfExisted(tableName)
          }

          val agendaJobId = get("agendajob_id")
          if (agendaJobId.nonEmpty) injector.instanceOf[AgendaJobService].cancelJob(agendaJobId)

          markAsDeleted(dbObject)
          (true, "Table deleted successfully")
        } catch {
          case ex: Exception =>
            Logger.error("Failed to delete table", ex)
            writeErrorMessage(dbObject, ex.getMessage)
            (false, "Failed to delete table, please check the error message in DB")
        }
      case None =>
        val message = s"Record not found in DB"
        play.api.Logger.error(message)
        (false, message)
    }
  }

  def markAsDeleted(dbObject: DBObject): Unit = {
    val state = "DELETED"
    val now = new java.util.Date()
    val update = $set("state" -> state, "status" -> state, "updatedAt" -> now, "error" -> "")
    collection.update(dbObject, update)
  }

  def dropHiveRelatedTable(tableName: String, hiveSchema: String): Unit = {
    injector.instanceOf[HiveConnector].dropHiveTable(tableName, hiveSchema)
  }

  private def writeErrorMessage(dbObject: DBObject, error: String): Unit = {
    val update = $set("updatedAt" -> new java.util.Date(), "error" -> error)
    collection.update(dbObject, update)
  }
}
