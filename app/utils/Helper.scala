
package utils

import modules.GlobalContext.injector

/**
  * Helper methods.
  */
object Helper {

  /**
    * return the scheduler application root path
    */
  lazy val AppRootPath: String = {
    injector.instanceOf[play.Application].path.getAbsolutePath
  }

  /**
    * Write content into target local filesystem path.
    *
    * @param path the target local filesystem path.
    * @param content the content to be written.
    */
  def writeToFile(path: String, content: String): Unit = {
    import java.io.{BufferedWriter, File, FileWriter}

    val file = new File(path)
    file.getParentFile.mkdirs
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }

  /**
    * Convert raw string to escaped string.
    *
    * @param raw e.g.
    *   ?Hello
    *     World
    * @return
    *   \bHello\n\tWorld
    */
  def escape(raw: String): String = org.apache.commons.lang.StringEscapeUtils.escapeJava(raw)

  /**
    * Merge multi files in the same directory into one file.
    *
    * @param dir the target directory.
    * @param targetFile the full path of target file.
    * @return exit code. 0 if completed successfully.
    */
  def mergeBatchFiles(dir: String, targetFile: String): Int = {
    import scala.sys.process._

    // Wildcard * in string cannot be interpreted and expanded by shell
    val config = injector.instanceOf[play.Configuration]
    val shell = config.getString("CommandShell")
    val mergeCommand = Seq(shell, "-c", s"cat $dir/* > $targetFile")
    mergeCommand.!
  }

  case class NodeInfo(nodeName: String, host: String, loginUser: String)

  def getNodeList: Vector[NodeInfo] = {
    import scala.collection.JavaConverters._

    val config = injector.instanceOf[play.Configuration]
    config
      .getConfigList("ClusterNodes")
      .asScala
      .toVector
      .map { node =>
        NodeInfo(
          node.getString("name").toUpperCase,
          node.getString("host"),
          node.getString("loginUser")
        )
      }
  }

  /**
    * Convert Java Instant now to string and forrmats it. e.g.
    *   2018-05-04T09:07:08.050Z -> 20180504t090920502z
    *
    * @return formatted current time string.
    */
  def currentTime: String = java.time.Instant.now.toString.replaceAll("[-:.]", "").toLowerCase
}
