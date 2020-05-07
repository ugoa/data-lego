
package models

import javax.inject.Singleton

import com.mongodb.casbah.Imports._

object HiveTableInfo {

  case class Model(id: String)

  @Singleton
  class DAO extends CommonDAO[Model] with Droppable {

    override val mongoCollectionName: String = "hive_tables"
    override val dataSinkField: String = "data_sink"
    override val tableField: String = "hivetable_name"
    override val hiveSchemaField: String = "table_schema"

    override def toEntity(dbObject: DBObject): Model = {
      Model(id = dbObject("_id").toString)
    }
  }
}
