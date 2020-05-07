
package controllers

import java.sql.SQLException
import javax.inject.{Inject, Singleton}

import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import connectors.SapHanaConnector
import models.SapHanaConfig
import utils.Helper.escape

/**
  * TODO: Remove this controller and integrate SAP HANA service within [[controllers.JDBCController]]
  *
  * @param dao Injected instance of [[models.SapHanaConfig.DAO]]
  */
@Singleton
class SapHanaController @Inject() (dao: SapHanaConfig.DAO) extends  Controller {

  def getTables(confId: String) = Action {
    getConnector(confId) match {
      case Some(connector) =>
        try {
          val tableNames = connector.listTables.map(name => s""""$name"""").mkString(", ")
          val jsonStr = s"[$tableNames]"
          Ok(Json.parse(jsonStr))
        } catch {
          case ex: SQLException =>
            val err = s"Connection failed. Please check the record `$confId` in hanaconn_saved table."
            Logger.error(err, ex)
            UnprocessableEntity(err)
        }
      case None => NotFound(s"SapHana Config $confId not found in database")
    }
  }

  def getTopRows(confId: String, table_name: String) = Action {
    getConnector(confId) match {
      case Some(connector) =>
        val rows = connector.getTopRows(table_name)
        val serialzedRows = rows.map(row => {
          val pairStr =
            row
              .map { case (column, value) => s""""$column": "${escape(value.toString)}"""" }
              .mkString(", ")
          s"""{$pairStr}"""
        }).mkString(", ")

        val jsonStr = s"[$serialzedRows]"

        Ok(Json.parse(jsonStr))
      case None => NotFound(s"SapHana Config $confId not found in database")
    }
  }

  private def getConnector(confId: String): Option[SapHanaConnector] = {
    dao.findOneById(confId) match {
      case Some(hanaConfig) =>
        Some(new SapHanaConnector(
          hanaConfig.host,
          hanaConfig.port,
          hanaConfig.dbName,
          hanaConfig.dbSchema,
          hanaConfig.dbUser,
          hanaConfig.dbPassword
        ))
      case None => None
    }
  }
}

