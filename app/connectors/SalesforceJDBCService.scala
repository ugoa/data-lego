
package connectors

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import modules.GlobalContext.injector
import models.ConnectConfig

import play.api.Logger

class SalesforceJDBCService(salesforceConfigId: String) extends CommonJDBCService {

  private val dao = injector.instanceOf[ConnectConfig.DAO]

  override def getConnection(): Connection ={

    try{

      val jdbcConfig: ConnectConfig.Model = dao.findOneById(salesforceConfigId).get
      Class.forName("com.ddtek.jdbc.sforce.SForceDriver")
      val connStr = s"jdbc:datadirect:sforce://${jdbcConfig.host}"

      val props = new Properties()
      props.setProperty("user", jdbcConfig.dbUser)

      /**
      *  if the authentication needs a security token then the token should be concatenated to the password or
      *  passed through the SecurityToken property
      *  e.g. secretXXXX (XXXX being the token)
      */

      props.setProperty("password",jdbcConfig.dbPassword)
      props.setProperty("SecurityToken", jdbcConfig.properties("token"))
      props.setProperty("Database", jdbcConfig.dbName)
      props.setProperty("TransactionMode", "ignore")
      DriverManager.getConnection(connStr, props)
    } catch {
      case ex: SQLException =>
        Logger.error(s"Salesforce Connection Failed. ${ex.toString}")
        throw ex
    }
  }

  override val schemaSQL: String = "SELECT schema_name from information_schema.system_schemata"
}