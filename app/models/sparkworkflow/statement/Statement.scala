
package models.sparkworkflow.statement

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.Statement.Output._
import models.sparkworkflow.statement.Statement.{ValPrefix, CodeType}

object Statement {

  val ValPrefix = "output"

  object Output {
    val DataFrame = "DataFrame"
    val Model = "Model"
    val Transformer = "Transformer"
    val Estimator = "Estimator"
    val Evaluator = "Evaluator"
    val MetricValue = "MetricValue"
    val Report = "Report"
    val TempView = "TempView"

    val SupportedOutputs =
      Set(DataFrame, Model, Transformer, Estimator, Evaluator, MetricValue, Report, TempView)
  }

  object CodeType {
    val Spark = "spark"
    val PySpark = "pyspark"
    val SparkR = "sparkr"
    val SQL = "sql"

    val SupportedSnippetKinds = Set(Spark, PySpark, SparkR, SQL)
  }
}

/**
  * The abstract class of all the statements, where `statement` is essentially a code snippet builder.
  * It takes an node [[models.SparkWorkflow.Node]], extract/validate the params and apply the param values
  * to the snippet template within each statement. A workflow[[models.SparkWorkflow]] contains
  * a sequences of nodes [[models.SparkWorkflow.Model.nodes]], each of which would be translated into
  * a valid code snippet of Spark job. Combines all the snippets together orderly would you get a whole
  * Spark job that should run successfully in a shark-shell or by Livy server.
  */
abstract class Statement(node: Node) {

  /**
    * The list of the parent output types, whose value are limited in range of [[Statement.Output]]
    */
  def parentOutputTypes: Vector[String]

  /**
    * The list of self output types, whose value are limited in range of [[Statement.Output]]
    */
  def selfOutputTypes: Vector[String]

  /**
    * The list of required keys of the parameters [[models.SparkWorkflow.Node.parameters]]
    */
  def paramKeys: Vector[String]

  /**
    * The code snippet to be produced.
    */
  def snippet: String

  /**
    * Default parameter value holder.
    */
  def formattedParameters: Map[String, String] = node.parameters

  /**
    * Return the variable name of the Dataframe output at given position
    * @param outputId the id of self output list.
    * @return The variable name if the node produce a dataframe at given output Id, else returns None
    */
  def getDataFrameOutput(outputId: Int): Option[String] = {
    val valName = getOutputName(node.orderId, outputId, DataFrame)
    selfOutputs.find(_ == valName)
  }

  /**
    * Code snippet type.
    * @return
    */
  def codeType: String = CodeType.Spark

  /**
    * Helper method to validate if all given keys have non-empty values.
    * @param keys keys of formattedParameters to be validated.
    */
  protected final def requireNonEmpty(keys: Vector[String]): Unit = {
    keys.foreach { key =>
      require(
        formattedParameters(key) != "",
        s"$concreteStatement: content of`$key` CANNOT be empty"
      )
    }
  }

  /**
    * Helper method to validate if all given keys have boolean values.
    * @param keys keys of formattedParameters to be validated.
    */
  protected final def requireBoolean(keys: Vector[String]): Unit = {
    keys.foreach { key =>
      val value: String = formattedParameters(key)
      require(
         value == "true" || value == "false",
        s"$concreteStatement: Expected content of `$key` to be 'true' or 'false', got: '$value'"
      )
    }
  }

  /**
    * Helper method to validate if all given keys have numeric values.
    * @param keys keys of formattedParameters to be validated.
    */
  protected final def requireNumeric(keys: Vector[String]): Unit = {
    keys.foreach { key =>
      val value: String = formattedParameters(key)
      require(
        try { value.toDouble; true } catch { case _: Exception => false },
        s"$concreteStatement: Expected content of `$key` to be numeric, got: '$value'"
      )
    }
  }

  /**
    * Helper method to validate if all given keys have integer values.
    * @param keys keys of formattedParameters to be validated.
    */
  protected final def requireInteger(keys: Vector[String]): Unit = {
    keys.foreach { key =>
      val value: String = formattedParameters(key)
      require(
        try { value.toInt; true } catch { case _: Exception => false },
        s"$concreteStatement: Expected content of `$key` to be integer, got: '$value'"
      )
    }
  }

  /**
    * Get the concrete statement name.
    */
  protected final lazy val concreteStatement: String = this.getClass.getSimpleName

  /**
    * Generate the list of parent output variables by the parentOutputTypes
    */
  protected final lazy val parentOutputs: Vector[String] = {
    if (parentOutputTypes.size == parentOutputTypes.toSet.size) {
      // If the parents have different output types with each other,
      // which most of them are, the order of them are irrevelant. We can get outpout val just by type
      parentOutputTypes.map { item  =>
        val parent = node.parentOutputs.find(_.outputType == item).get
        getOutputName(parent.parentId, parent.outputId, parent.outputType)
      }
    } else {
      // Otherwise, Get the parent outputs strictly by order
      // A example is `Join` node have two parents with same type as DataFrame
      node.parentOutputs.map(p => getOutputName(p.parentId, p.outputId, p.outputType))
    }
  }

  /**
    * Generate the list of self output variables by the selfOutputTypes
    */
  protected final lazy val selfOutputs: Vector[String] = {
    (1 to selfOutputTypes.length).map { outputId =>
      getOutputName(node.orderId, outputId, selfOutputTypes(outputId - 1))
    }.toVector
  }

  /**
    * Name of the method that serve as a naming space to avoid naming conflict.
    */
  protected final lazy val scopingMethod: String = s"fn${node.orderId}_$concreteStatement"

  /**
    * Get value by the given key from formattedParameters
    * @param key the key to be restrived.
    * @return the value of the key.
    */
  protected final def get(key: String): String = {
    require(paramKeys.contains(key), s"key $key is not defined in $concreteStatement#paramKeys")
    formattedParameters(key).trim
  }

  /**
    * Get value by the given key from node original parameters.
    * @param key the key to be restrived.
    * @return the value of the key.
    */
  protected final def getRaw(key: String): String = {
    require(paramKeys.contains(key), s"key $key is not defined in $concreteStatement#paramKeys")
    node.parameters(key)
  }

  /**
    * Get generated output variable name based on the node id, output id and output type.
    * @param nodeId the order id of the node [[models.SparkWorkflow.Node.orderId]]
    * @param outputId the self/parent output id of the node.
    * @param outputType the self/parent output type of the node.
    * @return the variable name in the code snippet.
    */
  protected final def getOutputName(nodeId: Int, outputId: Int, outputType: String): String = {
    require(
      SupportedOutputs.contains(outputType),
      s"Output type `$outputType` is not supported, must be one of ${SupportedOutputs.mkString(", ")}."
    )
    ValPrefix + nodeId + outputType + outputId
  }

  require(
    parentOutputTypes.sorted == node.parentOutputs.map(_.outputType).sorted,
    s"Miss parent port ${parentOutputTypes.mkString(", ")} for $concreteStatement"
  )

  private lazy val nodeParamKeySet: Set[String] = formattedParameters.keys.toSet
  require(
    paramKeys.forall(nodeParamKeySet),
    s"Miss parameters ${paramKeys.filterNot(nodeParamKeySet).mkString(", ")} for $concreteStatement"
  )

  require(
    node.outputs.sorted == selfOutputTypes.sorted, // Convert to Set to ignore sequence
    s"Mismatch of Outputs defined between node in DB and in $concreteStatement statement"
  )

  require(
    selfOutputs.forall(snippet.contains),
    s"""
       |Miss defining ${selfOutputs.filterNot(snippet.contains).mkString(", ")}
       |in generated code snippet for $concreteStatement
       |""".stripMargin.replaceAll("\n", " ")
  )

  private def estimatorParentColParamReformatted_? : Boolean = {
    val allParentTypes = node.parentOutputs.map(_.outputType)

    if (allParentTypes.exists(Set(Estimator, Evaluator))) this.isInstanceOf[ExtractAndResetColParams]
    else true
  }

  require(
    estimatorParentColParamReformatted_?,
    s"""
       |$concreteStatement has Evaluator/Estimator parent therefore is required to
       |format the column params by extending `ExtractAndResetColParams` trait.
       |""".stripMargin.replaceAll("\n", " ")
  )
}
