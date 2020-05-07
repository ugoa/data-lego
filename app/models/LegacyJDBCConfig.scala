
package models

trait LegacyJDBCConfig {
  val collectionName: String

  case class Model(
    id: String,
    name: String,
    host: String,
    port: String,
    dbName: String,
    dbSchema: String,
    dbUser: String,
    dbPassword:String
  )
}
