
package commands

import modules.GlobalContext.injector

/**
  * Object to build shell commands for uploading file from local filesystem where the scheduler is running
  * to the HDFS of the Hadoop claster, or removing file/directory on HDFS cluster.
  *
  * TODO: Investigate secure HDFS client with Java.
  * @see <a href="https://community.hortonworks.com/articles/56702/a-secure-hdfs-client-example.html">
  *
  */
object FileUploadCommandBuilder {

  /**
    * SSH credentials of HDFS cluster for file uploading.
    */
  private val sshEnv = {
    val config = injector.instanceOf[play.Configuration]
    val sshUrl: String = config.getString("SSHHdfsUrl")
    val sshUserName: String = config.getString("SSHHdfsUserName")
    s"$sshUserName@$sshUrl"
  }

  /**
    * Commands builder for the file uploading process.
    *
    * @param tempFileName The name of file to be uploaded.
    * @param fileLocalPath the local filesystem path of the file.
    * @param removeHeader Indicate whether the file has header, mainly for CSV file.
    * @param hdfsFilePath The destinated HDFS directory on cluster when the file to be uploaded into.
    * @return The valid command string.
    */
  def uploadCommand(
      tempFileName: String,
      fileLocalPath: String,
      removeHeader: Boolean,
      hdfsFilePath: String): String = {

    val remoteTempFile = s"/tmp/$tempFileName"

    // val addCustomHeader = if (customHeader.nonEmpty) s"sed -i '1 i\\$customHeader' $filePath" else ""
    val removeOriginalHeaderIfAny = if (removeHeader) s"sed -i '1d' $fileLocalPath" else ""

    // local file
    // -> (copy to remote /tmp/file by scp) remote:/tmp/file
    // -> (upload to hdfs) 'hdfs path'
    // delete local file
    s"""
       |$removeOriginalHeaderIfAny
       |scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $fileLocalPath $sshEnv:$remoteTempFile
       |ssh -tt $sshEnv "
       |sudo chown hdfs:hadoop $remoteTempFile
       |sudo runuser -l hdfs -c 'hdfs dfs -moveFromLocal -f $remoteTempFile $hdfsFilePath'
       |"
       |rm -f $fileLocalPath
       |""".stripMargin
  }


  /**
    * Generates command to delete HDFS a file/directory on cluster
    *
    * @param hdfsFilePath the HDFS file/directory to be deleted.
    * @return The valid command string.
    */
  def rmHDFSFileCommand(hdfsFilePath: String): String = {
    val rmIfExist = s"if hdfs dfs -test -e $hdfsFilePath; then hdfs dfs -rm $hdfsFilePath; fi"

    s""" ssh -tt $sshEnv "sudo runuser -l hdfs -c '$rmIfExist'" """
  }
}
