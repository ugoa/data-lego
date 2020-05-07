
package models

import javax.inject.Singleton
import com.mongodb.casbah.Imports._

object ConnectConfig {

  /**
    * External JDBC datastore configuration.
    *
    * @param id The identifier.
    * @param name The description.
    * @param host The host of JDBC datastore.
    * @param port The port of JDBC datastore.
    * @param dbName The database name
    * @param dbUser The database user
    * @param dbPassword The database password.
    * @param properties The extra JDBC connection options for different datastores.
    * @param source The JDBC protocol type of the JDBC datastore.
    *               e.g. "jdbc:postgresql:..." where the postgresql is the source.
    */
  case class Model(
    id: String,
    name: String,
    host: String,
    port: String,
    dbName: String,
    dbUser: String,
    dbPassword:String,
    properties: Map[String, String],
    source: String
  )

  @Singleton
  class DAO extends CommonDAO[Model] {
    override val mongoCollectionName: String = "ConnectConfigs"

    override def toEntity(dbObject: DBObject): Model  = {
      val get = Helper.get(dbObject, _: String)
      Model(
        id = dbObject("_id").toString,
        name = get("connectionName"),
        host = get("host"),
        port = get("port"),
        dbName = get("dbName"),
        dbUser = get("dbUser"),
        dbPassword = get("dbPassword"),
        properties = convertToMap(dbObject, "properties"),
        source = get("source")
      )

    }
  }
}
