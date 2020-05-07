
package models

import java.time.Instant
import java.util.Date

import com.mongodb.casbah.Imports._

object Helper {

  def get(dbObject: DBObject, field: String): String = dbObject.getAs[String](field).getOrElse("")

  def getBoolean(dbObject: DBObject, field: String): Boolean = {
    dbObject.getAs[Boolean](field).getOrElse(false)
  }

  def getInt(dbObject: DBObject, field: String): Int = dbObject.getAs[Int](field).getOrElse(0)

  def getList(dbObject: DBObject, field: String): MongoDBList = {
    dbObject.getAs[MongoDBList](field).getOrElse(new MongoDBList)
  }

  def getDate(dbObject: DBObject, field: String): Instant = {
    dbObject.getAs[Date](field) match {
      case Some(date) => date.toInstant
      case None => Instant.MIN
    }
  }


  def toUpperCase(str: String): String = str.replaceAll("[-_\\s]", "").toUpperCase

  def tableFormat(tableName: String): String = tableName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase

  def v(dbObject: DBObject, field: String): String = dbObject.getAs[String](field).getOrElse("")
}
