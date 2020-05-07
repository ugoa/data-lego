
package models

import java.util.Date
import java.time.Instant

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.mongodb.casbah.MongoCollection

import connectors._
import org.bson.types.ObjectId
import com.mongodb.casbah.Imports._

import models.Helper.get

trait CommonDAO[M] {

  val mongoCollectionName: String

  def toEntity(dbObject: DBObject): M

  lazy val collection: MongoCollection = MongoConnector.collection(mongoCollectionName)

  def findOneById(id: String): Option[M] = collection.findOneByID(new ObjectId(id)).map(toEntity)

  def findOneBy(field: String, value: Any): Option[M] = {
    if (field == "_id") findOneById(value.toString)
    else collection.findOne(DBObject(field -> value)).map(toEntity)
  }

  def findBatch(query: DBObject): Set[M] = collection.find(query).map(toEntity).toSet

  def findOneDBObjectById(id: String): Option[DBObject] = collection.findOneByID(new ObjectId(id))

  def findOneDBObjectBy(field: String, value: String): Option[DBObject] = {
    if (field == "_id") collection.findOneByID(new ObjectId(value))
    else collection.findOne(DBObject(field -> value))
  }

  def updateStatus(objectId: String, state: String, error: String = ""): Unit = {
    val err = if (error.nonEmpty) s"${Instant.now}: $error" else ""
    val map = Map("state" -> state, "error" -> err, "updatedAt" -> new Date)
    updateFields(objectId, map)
  }

  def updateFields(objectId: String, fieldsMapping: Map[String, Any]): Unit = {
    val update = $set(fieldsMapping.toList: _*)
    collection.update(getDBObject(objectId), update)
  }

  def updateField(objectId: String, field: String, value: Any): Unit = {
    val update = $set(field -> value)
    collection.update(getDBObject(objectId), update)
  }

  def deleteFields(objectId: String, fieldsMapping: List[String]): Unit = {
    val unset = $unset(fieldsMapping: _*)
    collection.update(getDBObject(objectId), unset)
  }

  def deleteField(objectId: String, field: String): Unit = {
    val unset = $unset(field)
    collection.update(getDBObject(objectId), unset)
  }

  protected def convertToOrderedMap(
      dbObject: MongoDBObject, field: String): mutable.LinkedHashMap[String, String] = {

    val map = mutable.LinkedHashMap[String, String]()
    dbObject.get(field).foreach { obj =>
      val dbObj = obj.asInstanceOf[DBObject]
      val keys = dbObj.toMap.keySet.asScala
      keys.foreach(key => map(key.toString) = get(dbObj, key.toString))
    }
    map
  }

  protected def convertToMap(dbObject: MongoDBObject, field: String): Map[String, String] = {

    val map = mutable.Map[String, String]()
    dbObject.get(field).foreach { obj =>
      val dbObj = obj.asInstanceOf[DBObject]
      val keys = dbObj.toMap.keySet.asScala
      keys.foreach(key => map(key.toString) = get(dbObj, key.toString))
    }
    map.toMap
  }

  protected def getDBObject(id: String): MongoDBObject = DBObject("_id" -> new ObjectId(id))
}
