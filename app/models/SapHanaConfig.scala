
package models

import javax.inject.Singleton

import com.mongodb.casbah.Imports._

object SapHanaConfig {

  case class Model(
    id: String,
    name: String,
    host: String,
    port: String,
    dbName: String,
    dbSchema: String,
    dbUser: String,
    dbPassword:String
  )

  @Singleton
  class DAO extends CommonDAO[Model] {
    override val mongoCollectionName = "hanaconn_saved"

    override def toEntity(dbObject: DBObject): Model = {
      val get = Helper.get(dbObject, _: String)

      Model(
        id = dbObject("_id").toString,
        name = get("source"),
        host = get("host"),
        port = get("port"),
        dbName = get("databaseName"),
        dbSchema = get("schemaName"),
        dbUser = get("user"),
        dbPassword = get("pwd")
      )
    }
  }
}
