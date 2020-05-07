
package controllers

import java.sql.SQLException

import play.api.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Writes}
import play.api.mvc.{Action, Controller}

import connectors.{CommonJDBCService, TableCell, TableMetaData}
import utils.Helper.escape

trait JDBCController extends Controller {

  def getSchemas(service: CommonJDBCService) = Action {
    try {
      val schemaNames = service.listSchemas().map(name => s""""$name"""").mkString(", ")
      val jsonStr = s"[$schemaNames]"
      Ok(Json.parse(jsonStr))
    } catch {
      case ex: SQLException =>
        val err = s"Failed to get schema. ${ex.toString}"
        Logger.error(err, ex)
        UnprocessableEntity(err)
    }
  }

  def getTables(service: CommonJDBCService, schemaName: String) = Action {
    try {
      val tableNames = service.listTables(schemaName).map(name => s""""$name"""").mkString(", ")
      val jsonStr = s"[$tableNames]"
      Ok(Json.parse(jsonStr))
    } catch {
      case ex: SQLException =>
        val err = s"Connection failed."
        Logger.error(err, ex)
        UnprocessableEntity(err)
    }
  }

  def getTopRows(service: CommonJDBCService, schemaName: String, tableName: String) = Action {
    try {
      val rows = service.getTopRows(schemaName, tableName)
      val serialzedRows = rows.map(row => {
        val pairStr =
          row.map { case TableCell(column, value) =>
            s""""$column": "${escape(value.toString)}""""
          }.mkString(", ")
        s"""{$pairStr}"""
      }).mkString(", ")

      val jsonStr = s"[$serialzedRows]"

      Ok(Json.parse(jsonStr))
    } catch {
      case ex: SQLException =>
        val err = s"Failed to get top rows"
        Logger.error(err, ex)
        UnprocessableEntity(err)
    }
  }

  implicit val tableMetaDataWrites: Writes[TableMetaData] = (
    (JsPath \ "COLUMN_NAME").write[String] and
      (JsPath \ "DATA_TYPE").write[String]
    )(unlift(TableMetaData.unapply))

  def getMetaData(service: CommonJDBCService, schemaName: String, tableName: String) = Action {
    try {
      val mdList = service.getTableMetaData(schemaName, tableName)
      Ok(Json.obj("rows" -> Json.toJson(mdList)))
    } catch {
      case ex: SQLException =>
        val err = s"Failed to get metadata for table"
        Logger.error(err, ex)
        UnprocessableEntity(err)
    }
  }
}
