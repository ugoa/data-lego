
package connectors

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import modules.GlobalContext.injector
import models.ConnectConfig

import play.api.Logger

class RedshiftJDBCService(redshiftConfigId: String) extends CommonJDBCService {

  private val dao = injector.instanceOf[ConnectConfig.DAO]

  override def getConnection(): Connection = {

    try{
      val jdbcConfig: ConnectConfig.Model = dao.findOneById(redshiftConfigId).get

      Class.forName("com.amazon.redshift.jdbc42.Driver")
      val connStr = s"jdbc:redshift://${jdbcConfig.host}/${jdbcConfig.dbName}"

      val props = new Properties()
      jdbcConfig.properties.foreach { case (k, v) => props.setProperty(k, v) }
      props.setProperty("user", jdbcConfig.dbUser)
      props.setProperty("password", jdbcConfig.dbPassword)

      DriverManager.getConnection(connStr, props)
    } catch {
      case ex: SQLException =>
        Logger.error(s"Redshift Connection Failed. ${ex.toString}")
        throw ex
    }
  }

  override val schemaSQL: String = "select * from pg_namespace"
}
