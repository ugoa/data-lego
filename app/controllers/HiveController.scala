
package controllers

import javax.inject.{Inject, Singleton}

import connectors.HiveConnector

@Singleton
class HiveController @Inject()(service: HiveConnector) extends JDBCController {

  def getSchemas(runAs: Option[String]) = {
    if (runAs.isDefined) service.setUserName(runAs.get)
    super.getSchemas(service)
  }

  def getTables(schemaName: String, runAs: Option[String]) = {
    if (runAs.isDefined) service.setUserName(runAs.get)
    super.getTables(service, schemaName)
  }

  def getTableMetaData(schemaName: String, tableName: String, runAs: Option[String]) = {
    if (runAs.isDefined) service.setUserName(runAs.get)
    super.getMetaData(service, schemaName, tableName)
  }

  def getTopRows(schemaName: String, tableName: String, runAs: Option[String]) = {
    if (runAs.isDefined) service.setUserName(runAs.get)
    super.getTopRows(service, schemaName, tableName)
  }
}
