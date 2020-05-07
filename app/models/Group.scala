
package models

import javax.inject.Singleton

import com.mongodb.casbah.Imports._

object Group {

  case class Model(
    id: String,
    name: String,
    hiveSchema: String
  )

  @Singleton
  class DAO extends CommonDAO[Model] {

    override val mongoCollectionName: String = "groups"

    override def toEntity(dbObject: DBObject): Model = {
      val get = Helper.v(dbObject, _: String)

      Model(
        id = dbObject("_id").toString,
        name = get("group_name"),
        hiveSchema = get("group_schema")
      )
    }
  }
}
