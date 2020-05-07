
package models

import javax.inject.Singleton

import com.mongodb.casbah.Imports._
import common.{Field, SimpleField}

import modules.GlobalContext.injector

object DataMart {

  object Model {

    val ValidDataSources = Set("ODATA", "SQLQUERY", "CSV")
    val ValidDataSinks = Set("ELASTIC", "HIVE")
    val ValidOdataVersions = Set("V1", "V2", "V3", "V4")
  }

  case class Model(
      id: String,
      dataSource: String,
      dataSink: String,
      delimiter: String,
      tableName: String,
      hiveSchema: String,
      hasHeader: Boolean,
      characterEncoding: String,
      url: String,
      authType: String,
      username: String,
      password: String,
      tokenUrl: String,
      appKey: String,
      appSecret: String,
      odataVersion : String,
      newDataFields: Vector[Field]) {

    validateData(Model.ValidDataSources, dataSource)
    validateData(Model.ValidDataSinks, dataSink)

    if (dataSource == "ODATA") {
      validateData(Model.ValidOdataVersions, odataVersion)
    }

    val dataMartType: String = s"DATAMART_${dataSource}_$dataSink"

    val simpleFields: Vector[SimpleField] =
      newDataFields.map { field => SimpleField(field.name, field.fieldType) }

    private def validateData(validList: Set[String], value: String): Unit = {
      require(
        validList.contains(value),
        s"Invalid data for datamart $id. Expected: one of ${validList.mkString(", ")}, Got: $value"
      )
    }
  }

  @Singleton
  class DAO extends CommonDAO[Model] with Droppable {

    override val mongoCollectionName: String = "data_upload"
    override val dataSinkField: String = "data_sink"
    override val tableField: String = "table_name"
    override val hiveSchemaField: String = "table_schema"

    /**
      * overrides Droppable#dropTable, udpate sqlquery state too if data source is "SQLQUERY"
      */
    override def dropTable(objectId: String): (Boolean, String) = {

      super.dropTable(objectId) match {
        case (true, msg) =>
          findOneDBObjectById(objectId).foreach(dbObject => {
            val get = Helper.get(dbObject, _: String)
            if (Helper.toUpperCase(get("data_source")) == "SQLQUERY") markSqlQueryAsDeleted(get("query_id"))
          })
          (true, msg)
        case fail => fail
      }
    }

    override def toEntity(dbObject: DBObject): Model = {
      val get = Helper.v(dbObject, _: String)
      val toUpperCase = Helper.toUpperCase _

      val newDataFields: Vector[Field] = {
        val dataFields = convertToOrderedMap(dbObject, "datafields")
        val dataFieldMappings = convertToMap(dbObject, "datafield_mappings")
        val dateFieldFormats = convertToMap(dbObject, "datefield_formats")

        dataFields.map { case (fieldName, fieldType) =>
          val originName = dataFieldMappings.getOrElse(fieldName, fieldName)
          val datePattern = dateFieldFormats.get(fieldName)
          Field(fieldName, originName, fieldType, datePattern)
        }.toVector
      }

      Model(
        id = dbObject("_id").toString,
        dataSource = toUpperCase(get("data_source")),
        dataSink = toUpperCase(get("data_sink")),
        delimiter = get("delimiter"),
        tableName = Helper.tableFormat(get(tableField)),
        hiveSchema = get(hiveSchemaField),
        hasHeader = dbObject.getOrElse("has_header", "").toString == "true",
        characterEncoding = get("characterEncoding"),
        url = get("url"),
        authType = toUpperCase(get("auth_type")),
        username = get("username"),
        password = get("password"),
        tokenUrl = get("token_url"),
        appKey = get("app_key"),
        appSecret = get("app_secret"),
        odataVersion = toUpperCase(get("odataVersion")),
        newDataFields = newDataFields
      )
    }

    private def markSqlQueryAsDeleted(sqlQueryId: String): Unit = {
      val sqlQueryDAO = injector.instanceOf[SqlQuery.DAO]
      sqlQueryDAO.findOneDBObjectById(sqlQueryId) match {
        case Some(sqlDBObject) => sqlQueryDAO.markAsDeleted(sqlDBObject)
        case None =>
      }
    }
  }
}
