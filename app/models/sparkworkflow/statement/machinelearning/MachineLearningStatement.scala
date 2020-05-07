
package models.sparkworkflow.statement.machinelearning

import models.SparkWorkflow.Node
import models.sparkworkflow.statement.{SetterChainBuilder, Statement}
import models.sparkworkflow.statement.Statement.Output.Estimator

/**
  * Base class for all the ML statement node.
  */
abstract class MachineLearningStatement(node: Node) extends Statement(node) with SetterChainBuilder {

  def subPackage: String

  def paramSetMethodMappings: Vector[(String, String, String)]

  override final def getParam(key: String): String = super.get(key)

  override final lazy val parentOutputTypes = Vector()

  override lazy val selfOutputTypes: Vector[String] = Vector(Estimator)

  override final lazy val paramKeys: Vector[String] = paramSetMethodMappings.map(_._1)

  override final lazy val snippet: String = {

    val setMethodsChain = buildSetMethodChain(paramSetMethodMappings)
    val output = selfOutputs(0)

    s"val $output = new org.apache.spark.ml.$subPackage()$setMethodsChain"
  }
}

