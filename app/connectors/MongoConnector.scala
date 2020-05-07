
package connectors

import com.mongodb.casbah.Imports.ServerAddress
import com.mongodb.casbah._
import modules.GlobalContext.injector

object MongoConnector {

  private val config = injector.instanceOf[play.Configuration]

  private val DBUrl = config.getString("MongoDBURL")
  private val DBUserName = config.getString("MongoDBUsername")
  private val DBPassword = config.getString("MongoDBPassword").toArray
  private val DBName = config.getString("MongoDBDatabase")
  private val DBPort = 27017

  private lazy val client = {
    val mongoServer = new ServerAddress(DBUrl, DBPort)
    val mongocredentials = MongoCredential.createCredential(DBUserName, DBName, DBPassword)
    MongoClient(mongoServer, List(mongocredentials))
  }

  private lazy val db = client(DBName)

  def collection(collectionName: String): MongoCollection = db(collectionName)
}
