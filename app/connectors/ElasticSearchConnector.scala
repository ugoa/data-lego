
package connectors

import javax.inject.Inject

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.settings.Settings
import commands.{CommandProcessor, FileUploadCommandBuilder}
import common.SimpleField
import utils.Helper.currentTime

/**
  * ElasticSearch Connector to interact withe Elastic cluster.
  *
  * @param config Play application config.
  */
class ElasticSearchConnector @Inject()(config: play.Configuration) {

  private val settings =
    Settings
      .builder()
      .put("cluster.name", config.getString("elasticclustername"))
      .build()

  lazy val client =
    ElasticClient.transport(settings, ElasticsearchClientUri(config.getString("elasticsearchURI")))


  private val DefaultType = "query"

  private def rawMappings(properties: String) = {
    s"""
       |{
       |  "mappings": {
       |    "$DefaultType": {
       |      "dynamic_templates": [
       |        {
       |          "string_catcher": {
       |            "match_mapping_type": "string",
       |            "mapping": {
       |              "type": "string",
       |              "index": "not_analyzed"
       |            }
       |          }
       |        },
       |        {
       |          "int_catcher": {
       |            "match_mapping_type": "integer",
       |            "mapping": {
       |              "type": "integer"
       |            }
       |          }
       |        },
       |        {
       |          "double_catcher": {
       |            "match_mapping_type": "double",
       |            "mapping": {
       |              "type": "double"
       |            }
       |          }
       |        }
       |      ],
       |      "properties": {
       |        $properties
       |      }
       |    }
       |  }
       |}""".stripMargin
  }

  private def propertyJsonMappings(fieldTypes: Vector[SimpleField]) = {
    val properties =
      fieldTypes
        .map { case SimpleField(name, fieldType) =>
          if (fieldType == "string") s""""$name": { "type": "$fieldType", "index": "not_analyzed" }"""
          else s""""$name": { "type": "$fieldType" }"""
        }
        .mkString(",\n")

    rawMappings(properties)
  }

  /**
    * Delete a ES index.
    *
    * @param indexName
    */
  def deleteIndexIfExisted(indexName: String): Unit = {
    val doesIndexExists = client.execute { indexExists(indexName) }.await.isExists
    if (doesIndexExists) client.execute { ElasticDsl.deleteIndex(indexName)}.await
  }

  /**
    * Create a ES index, if the index is already there, delete it.
    * @param indexName the name of index to be created.
    */
  def createESIndex(indexName: String): Unit = {
    deleteIndexIfExisted(indexName)
    addIndex(indexName, Vector[SimpleField]())
  }

  /**
    * Safely create ES index with the content on HDFS.
    *
    * @param indexName the index name to be created.
    * @param hdfsFilePath the HDFS full path of the file.
    * @param fieldTypes the name-type list of the index fields.
    * @param importProcessFn The lambda of custom process for data writing into ES.
    *
    * @return Exit code. 0 indicate the process completed successfully.
    */
  def safeImportHDFSIntoIndex(
      indexName: String,
      hdfsFilePath: String,
      fieldTypes: Vector[SimpleField],
      importProcessFn: String => Int): Int = {

    try {
      safeImportToIndex(indexName, fieldTypes, importProcessFn)
    } finally {
      val cleanCmd = FileUploadCommandBuilder.rmHDFSFileCommand(hdfsFilePath)
      CommandProcessor.run(indexName, "CLEAN_HDFS_FILE", cleanCmd)
    }
  }

  /**
    * Safely create ES index. With any given process.
    * First create the index mapping,
    * Then create a temporary index to write data into.
    * If the process completed successfully, delete the index by the given {@param indexName},
    * create an alias of the temporary index with the given {@param indexName}.
    *
    * @param indexName the index name to be created.
    * @param fieldTypes the name-type list of the index fields.
    * @param importProcessFn The lambda of custom process for data writing into ES.
    *
    * @return Exit code. 0 indicate the process completed successfully.
    */
  def safeImportToIndex(
      indexName: String, fieldTypes: Vector[SimpleField], importProcessFn: String => Int): Int = {

    val formattedFieldTypes = fieldTypes.map { case SimpleField(fieldName, fieldType) =>
      val esDataType: String = fieldType match {
        case "bigint" => "long"
        case "int" => "integer"
        case "timestamp" => "date"
        case _ => fieldType
      }
      SimpleField(fieldName, esDataType)
    }

    val newIndex = s"${indexName}_version_$currentTime"

    addIndex(newIndex, formattedFieldTypes)
    val exitCode = importProcessFn(newIndex)

    if (exitCode == 0) {
      aliasIndex(indexName, newIndex)
    }
    exitCode
  }

  def close(): Unit = {
    client.close()
  }

  private def addIndex(indexName: String, fieldTypes: Vector[SimpleField]): Unit = {
    val fullMappings = propertyJsonMappings(fieldTypes)

    client.execute {
      createIndex(indexName).source(fullMappings)
    }.await
  }

  private def aliasIndex(alias: String, newIndex: String): Unit = {
    deleteIndexIfExisted(alias)
    client.execute { addAlias(alias) on newIndex }.await
  }
}
