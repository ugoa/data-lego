
package models.sparkworkflow.statement.setoperation

import models.sparkworkflow.statement.Statement
import models.sparkworkflow.statement.Statement.Output.DataFrame
import models.SparkWorkflow.Node

final class Split(node: Node) extends Statement(node) {

  override lazy val parentOutputTypes: Vector[String] = Vector(DataFrame)

  override lazy val selfOutputTypes: Vector[String] = Vector(DataFrame, DataFrame)

  override lazy val paramKeys: Vector[String] = Vector("mode", "ratio", "seed", "condition")

  private lazy val mode = get(paramKeys.head).toLowerCase
  private lazy val Vector(ratio, seed, condition) = paramKeys.takeRight(3).map(get)

  require(
    mode == "random" || mode == "conditional",
    s"Expected mode $concreteStatement to be neither 'random' or 'conditional'. Got: $mode"
  )

  require(
    ratioValid_?,
    s"Expected ratio $concreteStatement to be a double value between 0 and 1. Got: $ratio"
  )

  require(
    seedValid_?,
    s"Expected seed in $concreteStatement to be a int value between -1073741824 and 1073741823. Got: $seed"
  )

  require(
    !(mode == "conditional" && condition.isEmpty),
    s"Expected condition in $concreteStatement NOT to be empty if mode was conditional. Got: $ratio"
  )

  private def ratioValid_? : Boolean = {
    if (mode == "conditional") return true
    if (mode == "random" && ratio.isEmpty) return false
    try { 0 < BigDecimal(ratio) && BigDecimal(ratio) < 1 } catch { case _: Exception => false }
  }

  private def seedValid_? : Boolean = {
    if (mode == "conditional") return true
    if (mode == "random" && seed.isEmpty) return true
    try { -1073741824 < seed.toInt && seed.toInt < 1073741823 } catch { case _: Exception => false }
  }

  /**
    * NOTE: Currently there is a bug in Spark 2.1.x that DataFrame #except doesn't work with UDT columns.
    * It's has been fixed in Spark 2.2, however the latest Hortonworks 2.6.2
    * only supports Spark 2.1.1. The coming 2.6.3 will support Spark 2.2.
    *
    * So for now we will continue using #except and wait for the fix in Hortonworks 2.6.3/Spark 2.2 release
    *
    * See also:
    *   http://apache-spark-developers-list.1001551.n3.nabble.com/Spark-SQL-Dataframe-resulting-from-an-except-is-unusable-td20802.html
    *   https://github.com/apache/spark/pull/16765
    *   https://community.hortonworks.com/questions/138957/spark-220-release-in-hdp.html
    */
  override lazy val snippet: String = {

    val Vector(outputDF1, outputDF2) = selfOutputs
    val parentDataframe = parentOutputs(0)

    s"""
       |import org.apache.spark.sql.DataFrame
       |
       |def $scopingMethod(): (DataFrame, DataFrame) = {
       |  val mode = "$mode"
       |  val condition = "$condition"
       |  if (mode == "conditional") {
       |    val df1 = $parentDataframe.where(condition)
       |    val df2 = $parentDataframe.except(df1)
       |    (df1, df2)
       |  } else {
       |    val seed = "$seed"
       |    val (ratio1, ratio2) = ($ratio, (1 - BigDecimal($ratio)).toDouble)
       |    val Array(df1, df2) =
       |      if (seed.isEmpty) {
       |        $parentDataframe.randomSplit(Array(ratio1, ratio2))
       |      } else {
       |        $parentDataframe.randomSplit(Array(ratio1, ratio2), seed.toLong)
       |      }
       |    (df1, df2)
       |  }
       |}
       |
       |val ($outputDF1, $outputDF2) = $scopingMethod()
       |$outputDF1.cache()
       |$outputDF2.cache()
       |$outputDF1.count()
       |$outputDF2.count()
       |""".stripMargin
  }
}
