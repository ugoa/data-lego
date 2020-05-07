
package controllers

import javax.inject.Singleton
import connectors.{RedshiftJDBCService, SnowflakeJDBCService, SalesforceJDBCService, CommonJDBCService}

/**
  * Handle requests for all external JDBC queries.
  */

@Singleton
class CommonJDBCController extends JDBCController {

  private def getJDBCService(source: String, configId: String): CommonJDBCService = source.toUpperCase match {
    case "REDSHIFT" => new RedshiftJDBCService(configId)
    case "SNOWFLAKE" => new SnowflakeJDBCService(configId)
    case "SALESFORCE" => new SalesforceJDBCService(configId)
    case _ => throw new Exception(s"Unsupported data source: `$source`")
  }

  def getSchemas(source: String, confId: String) = {
    val service = getJDBCService(source, confId)
    super.getSchemas(service)
  }

  def getTables(source: String, confId: String, schemaName: String) = {
    val service = getJDBCService(source, confId)
    super.getTables(service, schemaName)
  }

  def getTopRows(source: String, confId: String, schemaName: String, tableName: String) = {
    val service = getJDBCService(source, confId)
    super.getTopRows(service, schemaName, tableName)
  }

  def getMetaData(source: String, confId: String, schemaName: String, tableName: String) = {
    val service = getJDBCService(source, confId)
    super.getMetaData(service, schemaName, tableName)
  }
}
