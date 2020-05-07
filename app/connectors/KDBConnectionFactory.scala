
package connectors

import java.sql.Types._
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import scala.collection.mutable.ArrayBuffer

import play.api.Logger

@Deprecated
class KDBConnectionFactory(
    host: String,
    port: String,
    dbName: String,
    dbSchema: String,
    dbUser: String,
    dbPassword: String) {

  Class.forName("jdbc")
  private val dbUrl = s"jdbc:q:$host:$port"

  def getConnection(): Connection = {
    try {
      DriverManager.getConnection(dbUrl, dbUser, dbPassword)
    } catch {
      case ex: SQLException =>
        Logger.error(s"JDBC Connection Failed. ${ex.toString}")
        throw ex
    }
  }

  def listTables: Seq[String] = {

    var tableNames = Array[String]()
    val conn = getConnection()
    try {
      val metaData = conn.getMetaData
      val resultSet = metaData.getTables(null, null, "%", Array("TABLE"))
      while (resultSet.next()) { tableNames :+= resultSet.getString(3) }
      resultSet.close()
    } finally conn.close()

    tableNames.toSeq
  }

  def getTopRows(tableName: String): Array[Array[(String, Any)]] = {
    val conn = getConnection()
    val statement = conn.createStatement()

    val TopNRows = 10

    try {
      val query = s"""SELECT * FROM \"$tableName\" LIMIT $TopNRows"""
      val resultSet = statement.executeQuery(query)
      val md = resultSet.getMetaData
      val columns = (1 to md.getColumnCount).map(md.getColumnName).toArray

      val result = ArrayBuffer[Array[(String, Any)]]()
      var i = 1
      while(resultSet.next() && i <= TopNRows) {
        val rowValues = (1 to columns.length).map(convertedValue(resultSet, _)).toArray
        val row = columns zip rowValues
        result += row
        i += 1
      }
      result.toArray
    } finally {
      statement.close()
      conn.close()
    }
  }

  private def getColumns(tableName: String): Array[String] = {
    val conn = getConnection()
    val statement = conn.createStatement()

    try {
      var columnList = ArrayBuffer[String]()
      val metaData = conn.getMetaData
      val resultSet = metaData.getColumns(null, null, tableName, "%")

      while (resultSet.next()) { columnList += resultSet.getString(4) }
      resultSet.close()

      columnList.toArray
    } finally {
      statement.close()
      conn.close()
    }
  }

  private def convertedValue(resultSet: ResultSet, index: Int): Any = {
    val colType = resultSet.getMetaData.getColumnType(index)

    val value =
      if (colType == CHAR) resultSet.getString(index)
      else if (colType == VARCHAR) resultSet.getString(index)
      else if (colType == LONGVARCHAR) resultSet.getString(index)
      else if (colType == NCHAR) resultSet.getString(index)
      else if (colType == NVARCHAR) resultSet.getString(index)
      else if (colType == LONGNVARCHAR) resultSet.getString(index)
      else if (colType == TINYINT) resultSet.getInt(index)
      else if (colType == SMALLINT) resultSet.getInt(index)
      else if (colType == INTEGER) resultSet.getInt(index)
      else if (colType == BIGINT) resultSet.getLong(index)
      else if (colType == FLOAT) resultSet.getFloat(index)
      else if (colType == REAL) resultSet.getFloat(index)
      else if (colType == DOUBLE) resultSet.getDouble(index)
      else if (colType == NUMERIC) resultSet.getBigDecimal(index)
      else if (colType == DECIMAL) resultSet.getBigDecimal(index)
      else if (colType == BIT) resultSet.getBoolean(index)
      else if (colType == DATE) resultSet.getDate(index)
      else if (colType == TIME) resultSet.getTime(index)
      else if (colType == TIMESTAMP) resultSet.getTimestamp(index)
      else if (colType == BINARY) resultSet.getByte(index)
      else if (colType == VARBINARY) resultSet.getByte(index)
      else if (colType == LONGVARBINARY) resultSet.getByte(index)
      else if (colType == ARRAY) resultSet.getArray(index)
      else if (colType == NULL) ""
      else {
        val typeName = resultSet.getMetaData.getColumnTypeName(index)
        throw new Exception(s"Unsupported JDBC data type $typeName")
      }

    if (value == null) "" else value
  }
}
