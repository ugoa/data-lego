
package models.sparkworkflow.statement.input
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.SparkWorkflow.Node

/**
  * Read data from AWS S3 to produce a DataFrame.
  */
final class ReadS3(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val paramKeys: Vector[String] = Vector("access_key", "secret_key", "file_path")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val dataFrameOutput = selfOutputs(0)
    val Vector(accessKey, secretKey, filePath) = paramKeys.map(get)

    s"""
       |spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "$accessKey")
       |spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "$secretKey")
       |val $dataFrameOutput = spark.sparkContext.textFile("s3n://$filePath").toDF
       |$dataFrameOutput.cache()
       |$dataFrameOutput.count()
       |""".stripMargin
  }
}

