
package models

import javax.inject.Singleton

import com.mongodb.casbah.Imports._

object KDBConfig extends LegacyJDBCConfig {
  override val collectionName: String = "kdbconn_saved"

  @Singleton
  class DAO extends CommonDAO[Model] {
    override val mongoCollectionName: String = collectionName

    override def toEntity(dbObject: DBObject): Model = {
      val get = Helper.get(dbObject, _: String)

      Model(
        id = dbObject("_id").toString,
        name = get("connectionName"),
        host = get("host"),
        port = get("port"),
        dbName = get("databaseName"),
        dbSchema = get("schemaName"),
        dbUser = get("user"),
        dbPassword = get("password")
      )
    }
  }
}
