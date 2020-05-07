
package models.sparkworkflow.statement.transformation.featureconversion

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.InputOutputColumnStatement
import models.sparkworkflow.statement.transformation.featureconversion.ConvertType._

/**
  * Convert the type of column(s).
  * TODO: Simplify the code template to just simple transformation, not the custom Transformer.
  */
final class ConvertType(node: Node) extends InputOutputColumnStatement(node) {

  override lazy val extraKeys: Vector[String] = ExtraKeys

  private lazy val targetType = get(ExtraKeys.head).toLowerCase

  require(
    SupportedTargetTypes.contains(targetType),
    s"$concreteStatement: Expect target_type to be one of ${SupportedTargetTypes.mkString(", ")}, got: $targetType"
  )

  override lazy val newStageMethodBody: String = {
    s"""
       |$ConvertTypeTransformerClassDefinition
       |new $ConvertTypeTransformerClass().setTargetType("$targetType")
       |""".stripMargin
  }
}

object ConvertType {

  /**
    * target_type must be one of string, boolean, integer, float, long, double, timestamp
    */
  private val ExtraKeys = Vector("target_type")

  private val SupportedTargetTypes =
    Set("string", "boolean", "integer", "float", "long", "double", "timestamp")
  
  private val ConvertTypeTransformerClass: String = "ConvertType"

  private val ConvertTypeTransformerClassDefinition: String =
    s"""
       |import org.apache.spark.ml.util.Identifiable
       |import org.apache.spark.ml.param.Param
       |import org.apache.spark.sql.{DataFrame, Dataset, Column}
       |import org.apache.spark.sql.types._
       |import org.apache.spark.ml.param.ParamMap
       |import org.apache.spark.ml.Transformer
       |
       |class $ConvertTypeTransformerClass(override val uid: String) extends Transformer {
       |
       |  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
       |  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
       |  final val targetType: Param[String] = new Param[String](this, "targetType", "target type name")
       |
       |  final def getInputCol: String = $$(inputCol)
       |  final def getOutputCol: String = $$(outputCol)
       |  final def getTargetType: String = $$(targetType)
       |
       |  final def setInputCol(value: String): this.type = set(inputCol, value)
       |  final def setOutputCol(value: String): this.type = set(outputCol, value)
       |  final def setTargetType(value: String): this.type = set(targetType, value)
       |
       |  private lazy val newSQLType = getTargetType match {
       |    case "string" => StringType
       |    case "boolean" => BooleanType
       |    case "timestamp" => TimestampType
       |    case "float" => FloatType
       |    case "long" => LongType
       |    case "double" => DoubleType
       |    case "integer" => IntegerType
       |  }
       |
       |  def this() = this(Identifiable.randomUID("converttype"))
       |
       |  override def transform(dataset: Dataset[_]): DataFrame = {
       |    dataset.withColumn(getOutputCol,  dataset.col(getInputCol).cast(newSQLType))
       |  }
       |
       |  override def copy(extra: ParamMap): $ConvertTypeTransformerClass = defaultCopy(extra)
       |
       |  override def transformSchema(schema: StructType): StructType = {
       |    val originField = schema.find(_.name == getInputCol).get
       |    val newSF = StructField(getOutputCol, newSQLType, originField.nullable, originField.metadata)
       |    schema.add(newSF)
       |  }
       |}
       |""".stripMargin
}
