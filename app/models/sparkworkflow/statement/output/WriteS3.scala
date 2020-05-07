
package models.sparkworkflow.statement.output

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame

/**
  * Write dataframe to AWS S3.
  */
final class WriteS3(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector()

  override lazy val paramKeys: Vector[String] = Vector("access_key", "secret_key", "file_path")

  requireNonEmpty(paramKeys)

  override lazy val snippet: String = {

    val parentDataFrame = parentOutputs(0)
    val Vector(accessKey, secretKey, filePath) = paramKeys.map(get)

    s"""
       |spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "$accessKey")
       |spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "$secretKey")
       |$parentDataFrame.rdd.saveAsTextFile("s3n://$filePath")
       |""".stripMargin
  }
}
