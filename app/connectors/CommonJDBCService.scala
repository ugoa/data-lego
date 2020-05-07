
package connectors

import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}
import java.sql.Types._

import scala.collection.mutable.ArrayBuffer

case class TableMetaData(columnName: String, columnType: String)
case class TableCell(columnName: String, value: Any)

/**
  * Trait for all the JDBC services.
  */
trait CommonJDBCService {

  /**
    * Interface to obtain a JDBC connection.
    * @return A JDBC connection.
    */
  def getConnection(): Connection

  /**
    * SQL script to get all the schemas of the given data source.
    * @return
    */
  def schemaSQL: String

  /**
    * Get all the schemas of the data source.
    * @return A list of all schemas.
    */
  def listSchemas(): Vector[String] = {
    val conn = getConnection()
    val statement: Statement = conn.createStatement()

    val schemas = ArrayBuffer[String]()
    try {
      val resultSet = statement.executeQuery(schemaSQL)
      while (resultSet.next()) { schemas += resultSet.getString(1) }
      schemas.toVector
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
    * Get all the table names with a given schema.
    *
    * @param schema The schema which to be queried with. Can be null if want to use the default schema.
    * @return A list of table names.
    */
  def listTables(schema: String = null): Vector[String] = {

    var tableNames = ArrayBuffer[String]()
    val conn = getConnection()
    try {
      val metaData = conn.getMetaData
      val resultSet = metaData.getTables(null, schema, "%", null)
      while (resultSet.next()) { tableNames += resultSet.getString(3) }
      resultSet.close()
    } finally conn.close()

    tableNames.toVector
  }

  /**
    * Get top 10 rows of the give table and schema.
    *
    * @param schema the schema to be queried
    * @param tableName the table to be queried.
    * @return A 2-dimensional list for each tabl cell.
    */
  def getTopRows(schema: String, tableName: String): Vector[Vector[TableCell]] = {
    val conn = getConnection()
    val statement = conn.createStatement()

    try {
      val query = s"""SELECT * FROM $schema.$tableName LIMIT 10"""
      val resultSet = statement.executeQuery(query)
      val md = resultSet.getMetaData
      val columns: Vector[String] = (1 to md.getColumnCount).map(md.getColumnName).toVector

      val result = ArrayBuffer[Vector[TableCell]]()
      while(resultSet.next()) {
        val rowValues: Vector[Any] = (1 to columns.length).map(convertedValue(resultSet, _)).toVector
        val row = (columns zip rowValues).map { case (colName, value) => TableCell(colName, value) }
        result += row
      }
      result.toVector
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
    * Get the metadata of a given schema/table.
    *
    * @param schema the schema to be queried
    * @param tableName the table to be queried.
    * @return A list of metadata.
    */
  def getTableMetaData(schema: String, tableName: String): List[TableMetaData] = {
    val conn = getConnection()
    val statement = conn.createStatement()

    try {
      val select = s"SELECT * FROM $schema.$tableName LIMIT 1"
      val rs: ResultSet = statement.executeQuery(select)
      val md: ResultSetMetaData = rs.getMetaData
      val result =
        (1 to md.getColumnCount)
          .map { i =>
            // Original name format is `tableName.colName`
            val colName = md.getColumnName(i).split("\\.").last
            TableMetaData(colName, md.getColumnTypeName(i))
          }.toList
      rs.close()
      result
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
      //Salesforce Types
      else if (colType == BOOLEAN) resultSet.getBoolean(index)
      else {
        val typeName = resultSet.getMetaData.getColumnTypeName(index)
        throw new Exception(s"Unsupported JDBC data type $typeName")
      }

    if (value == null) "" else value
  }
}
