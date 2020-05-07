
package connectors

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import modules.GlobalContext.injector
import models.ConnectConfig

import play.api.Logger

class SnowflakeJDBCService(snowflakeConfigId: String) extends CommonJDBCService {

  private val dao = injector.instanceOf[ConnectConfig.DAO]

  /**
    *  Example urls:
    *  For US West Region: jdbc:snowflake://{account_name}.snowflakecomputing.com/?{connection_params}
    *  Other Regions: jdbc:snowflake://{account_name}.{region_id}.snowflakecomputing.com/?{connection_params}
    *
    *  Valid region_id: (us-east-1,eu-west-1,eu-central-1,ap-southeast-2)
    *  Port isn't needed
    */

  override def getConnection(): Connection ={
    try{
      val jdbcConfig: ConnectConfig.Model  = dao.findOneById(snowflakeConfigId).get

      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
      val connStr = s"jdbc:snowflake://${jdbcConfig.host}/"

      val props = new Properties()
      props.setProperty("user", jdbcConfig.dbUser)
      props.setProperty("password", jdbcConfig.dbPassword)
      //props.setProperty("account", jdbcConfig.dbAccount)
      //props.setProperty("warehouse", jdbcConfig.dbWarehouse)
      props.setProperty("db", jdbcConfig.dbName)
      //props.setProperty("schema", jdbcConfig.dbSchema)

      DriverManager.getConnection(connStr, props)
    } catch {
      case ex: SQLException =>
        Logger.error(s"Snowflake Connection Failed. ${ex.toString}")
        throw ex
    }
  }

  override val schemaSQL: String = "SELECT schema_name from information_schema.schemata"
}