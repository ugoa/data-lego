
package models.sparkworkflow.statement

import models.sparkworkflow.statement.SetterChainBuilder.SetterInfo

object SetterChainBuilder {

  case class SetterInfo(paramKey: String, setterAPI: String, paramValueType: String)
}

/**
  * Builder that build a chain of setter methods.
  */
trait SetterChainBuilder {

  def getParam(key: String): String

  protected final def buildSetMethodChain(setterMappings: Vector[(String, String, String)]): String = {
    val setters: Vector[SetterInfo] =
      setterMappings.map { case (key, setterAPI, valueType) => SetterInfo(key, setterAPI, valueType) }

    newBuildSetMethodChain(setters)
  }

  /**
    * @param setMethodMappings
    *   param key | Spark component setAPI | param value type
    * @return
    *   String of `set` methods chain consists of the params whose value aren't empty.
    *   Add double quote around param value if the param type is NOT empty. e.g.
    *
    *     .setInputCol("name:labelCol").setMaxIter(10).setStandardization(false)
    */
  private def newBuildSetMethodChain(setMethodMappings: Vector[SetterInfo]): String = {

    setMethodMappings.collect { case SetterInfo(key, method, valueType) if getParam(key).nonEmpty =>

      checkValueType(valueType)

      val rawValue = getParam(key)
      val paramValue = valueType match {
        case "string" => s""""$rawValue""""
        case "double_array" | "integer_array" => s"Array($rawValue)"
        case _ => rawValue
      }
      s".$method($paramValue)"

    }.mkString("")
  }

  private def checkValueType(paramValueType: String): Unit = {

    val statement = this.getClass.getSimpleName

    val ValidValueTypes =
      Set("string", "integer", "double", "long", "boolean", "double_array", "integer_array")

    require(
      ValidValueTypes.contains(paramValueType),
      s"$statement: Param value type MUST be one of ${ValidValueTypes.mkString(", ")}, Got: $paramValueType"
    )
  }
}
