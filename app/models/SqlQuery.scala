
package models

import javax.inject.Singleton
import scala.collection.mutable

import com.mongodb.casbah.Imports._

import common.SimpleField

import models.Helper._

object SqlQuery {

  case class Model(
      id: String,
      runAs: String,
      fromAD: Boolean,
      dataflowID: String,
      script: String,
      sqlEngine: String,
      dataSink: String,
      connectHost: String,
      connectPort: String,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      destinationTable: String,
      tableSchema: String,
      fieldTypes: Vector[SimpleField],
      fileDestination: String,
      state: String,
      clusterNode: String) {

    private val ValidNodes: Vector[String] = ('A' to 'Z').toVector.map(_.toString)
    require(ValidNodes.contains(clusterNode), s"Node must be one of [A-Z]. Got: $clusterNode")

    val queryType: String = s"SQLQUERY_${dataSink}_$sqlEngine"

    def toBeCancelled_? : Boolean = state == "CANCELLING" || state == "ABORTING"

    def isRunning_? : Boolean = state == "RUNNING"

    def isQueued_? : Boolean = state == "QUEUED"

    def valid_? : Boolean = {
      Array("ELASTIC", "HIVE").contains(dataSink) && Array("SPARK").contains(sqlEngine)
    }
  }

  @Singleton
  class DAO extends CommonDAO[Model] with Droppable {

    override val mongoCollectionName: String = "SQLqueryresult_tmp"
    override val dataSinkField: String = "writer"
    override val tableField: String = "table_name"
    override val hiveSchemaField: String = "table_schema"

    override def toEntity(dbObject: DBObject): Model = {

      val dtypeList: Vector[SimpleField] =
        getList(dbObject, "field_types").toVector.map { element =>
          val fieldObj = element.asInstanceOf[DBObject]
          SimpleField(get(fieldObj, "label"), get(fieldObj, "type"))
        }

      val v = Helper.get(dbObject, _: String)

      val clusterNode = {
        val raw = v("cluster_node").toUpperCase
        if (raw == "") "A" else raw
      }

      Model(
        id = dbObject("_id").toString,
        runAs = v("run_as"),
        fromAD = getBoolean(dbObject, "from_ad") ,
        dataflowID = v("dataflow_id"),
        script = v("query"),
        sqlEngine = toUpperCase(v("sql_engine")),
        dataSink = toUpperCase(v("writer")),
        connectHost = v("connect_host"),
        connectPort = v("connect_port"),
        dbName = v("db_name"),
        dbUser = v("db_user"),
        dbPassword = v("db_password"),
        destinationTable = Helper.tableFormat(v(tableField)),
        tableSchema = v(hiveSchemaField),
        fieldTypes = dtypeList,
        fileDestination = v("file_destination"),
        state = toUpperCase(v("state")),
        clusterNode = clusterNode
      )
    }

    def updateQueryResultTable(id: String): Unit = {
      findOneById(id).foreach { model => updateField(id, "query_result.data", model.destinationTable) }
    }

    def updateFieldMappings(id: String, dataFields: mutable.LinkedHashMap[String, String]): Unit = {
      val dbList = new MongoDBList()
      dataFields.foreach { case (field, fieldType) =>
        dbList += MongoDBObject("type" -> fieldType, "label" -> field)
      }

      updateFields(id, Map("query_result.columns" -> dbList))
    }
  }
}
