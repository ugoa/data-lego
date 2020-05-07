
package controllers

import java.sql.SQLException
import javax.inject.{Inject, Singleton}

import connectors.KDBConnectionFactory
import models.KDBConfig
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

@Deprecated
@Singleton
class KDBController @Inject() (dao: KDBConfig.DAO) extends Controller {

  def getTables(confId: String) = Action {
    getConnector(confId) match {
      case Some(connector) =>
        try {
          val tableNames = connector.listTables.map(name => s""""${name}"""").mkString(", ")
          val jsonStr = s"[$tableNames]"
          Ok(Json.parse(jsonStr))
        } catch {
          case ex: SQLException =>
            val err = s"Connection failed. Please check the record `$confId` in kdbconn_saved table."
            Logger.error(err, ex)
            UnprocessableEntity(err)
        }
      case None => NotFound(s"KDB Config $confId not found in database")
    }
  }

  def getTopRows(confId: String, table_name: String) = Action {
    getConnector(confId) match {
      case Some(connector) =>
        val rows = connector.getTopRows(table_name)
        val serialzedRows = rows.map(row => {
          val pairStr =
            row.map { case (column, value) => s""""$column": "${value.toString}"""" }.mkString(", ")
          s"""{$pairStr}"""
        }).mkString(", ")

        val jsonStr = s"[$serialzedRows]"

        Ok(Json.parse(jsonStr))
      case None => NotFound(s"KDB Config $confId not found in database")
    }
  }

  private def getConnector(confId: String): Option[KDBConnectionFactory] = {
    dao.findOneById(confId) match {
      case Some(kdbConfig) =>
        Some(new KDBConnectionFactory(
          kdbConfig.host,
          kdbConfig.port,
          kdbConfig.dbName,
          kdbConfig.dbSchema,
          kdbConfig.dbUser,
          kdbConfig.dbPassword
        ))
      case None => None
    }
  }
}

