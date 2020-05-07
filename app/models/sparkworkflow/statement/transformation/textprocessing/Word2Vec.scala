
package models.sparkworkflow.statement.transformation.textprocessing

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.InputOutputColumnStatement.EmptyInputOutputParamMap
import models.sparkworkflow.statement.transformation.textprocessing.Word2Vec._

/**
  * @see <a href="https://spark.apache.org/docs/2.2.0/ml-features.html#word2vec">
  */
final class Word2Vec(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  override lazy val formattedParameters: Map[String, String] = {
    val originMap = OriginKeys.map(key => key -> getRaw(key)).toMap
    EmptyInputOutputParamMap ++ Map("operate_on" -> "one") ++ originMap
  }

  requireInteger(ExtraKeys.take(5))

  requireNumeric(ExtraKeys.takeRight(1))

  override lazy val newStageMethodBody: String = {
    s"""
       |import org.apache.spark.ml.feature.Word2Vec
       |new Word2Vec()
       |  .setVectorSize(${get("vector_size")})
       |  .setMinCount(${get("min_count")})
       |  .setMaxIter(${get("max_iterations")})
       |  .setStepSize(${get("step_size")}.toDouble)
       |  .setSeed(${get("seed")}.toLong)
       |  .setNumPartitions(${get("num_partitions")})
       |""".stripMargin
  }
}

object Word2Vec {

  private val ExtraKeys: Vector[String] =
    Vector("max_iterations", "seed", "vector_size", "num_partitions", "min_count", "step_size")

  private val OriginKeys: Vector[String] =
    Vector("input_column", "input_column_index", "output_mode", "append_column") ++ ExtraKeys
}
