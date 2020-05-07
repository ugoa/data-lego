
package models

import javax.inject.Singleton

import com.mongodb.casbah.Imports._
import models.Helper.get

object HiveExport {

  case class ProtocolConfig(host: String, user: String, password: String, destDir: String)

  case class Model(
    id: String,
    tableName: String,
    hiveSchema: String,
    protocol: String,
    protocolConfig: ProtocolConfig,
    delimiter: String
  )

  @Singleton
  class DAO extends CommonDAO[Model] {

    override val mongoCollectionName: String = "hive_exports"

    override def toEntity(dbObject: DBObject): Model = {

      val delimiter = {
        val origin = get(dbObject, "delimiter")
        if (origin.isEmpty) "," else origin
      }

      val configObject =
        dbObject
          .getAs[DBObject]("protocol_configs")
          .getOrElse(throw new Exception("'protocol_configs' fields not found."))

      val destDir = {
        val origin = get(configObject, "dest_dir")
        if (origin.last == '/') origin.dropRight(1) else origin
      }
      Model(
        id = dbObject("_id").toString,
        tableName = get(dbObject, "table_name"),
        hiveSchema = get(dbObject, "table_schema"),
        protocol = get(dbObject, "protocol"),
        protocolConfig =
          ProtocolConfig(
            host = get(configObject, "host"),
            user = get(configObject, "user"),
            password = get(configObject, "password"),
            destDir = destDir
          ),
        delimiter = delimiter
      )
    }
  }
}
