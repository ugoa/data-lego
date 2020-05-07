
package models

import java.time.Instant
import java.util.Date

import javax.inject.Singleton

import com.mongodb.casbah.Imports._
import org.bson.types.ObjectId

import models.Helper.{get, getDate}

object JobTransaction {

  case class Model(
      jobType: String,
      jobId: String,
      userId: String,
      startAt: Instant,
      endAt: Instant,
      state: String,
      log: String,
      triggerBy: String,
      createdAt: Instant,
      updatedAt: Instant,
      id: String = "") {

    require(Set("SPARKWORKFLOW", "INGESTIONJOB").contains(jobType), "Invalid job type")
    require(Set("USER", "SYSTEM").contains(triggerBy), "Invalid trigger method")
  }

  @Singleton
  class DAO extends CommonDAO[Model] {

    override val mongoCollectionName: String = "job_transactions"

    override def toEntity(dbObject: DBObject): Model = {

      Model(
        jobType = get(dbObject, "jobType"),
        jobId = get(dbObject, "jobId"),
        userId = get(dbObject, "userId"),
        startAt = getDate(dbObject, "startAt"),
        endAt = getDate(dbObject, "endAt"),
        state = get(dbObject, "state"),
        log = get(dbObject, "log"),
        triggerBy = get(dbObject, "triggerBy"),
        createdAt = getDate(dbObject, "createdAt"),
        updatedAt = getDate(dbObject, "updatedAt"),
        id = dbObject("_id").toString
      )
    }

    def createJobTransaction(model: Model): String = {
      val newId = ObjectId.get()
      val obj = DBObject(
        "jobType" -> model.jobType,
        "jobId" -> model.jobId,
        "userId" -> model.userId,
        "startAt" -> Date.from(model.startAt),
        "endAt" -> Date.from(model.endAt),
        "state" -> model.state,
        "log" -> model.log,
        "triggerBy" -> model.triggerBy,
        "createdAt" -> Date.from(model.createdAt),
        "updatedAt" -> Date.from(model.updatedAt),
        "_id" -> newId
      )

      collection.insert(obj)
      newId.toString
    }
  }
}
