
package models.sparkworkflow.statement

trait ExtractAndResetColParams {

  /**
    * Estimator/Evaluator have column param setters, such as setLabelCol, setFeaturesCol, etc,
    * which refer to an existing column name from the dataframe it about to be applied to,
    * normally this wouldn't be an issue if the input is the name of the column,
    * but we want to support index of the column too, which means user choose to provide
    * the name or index of the column. To meet this requirement, we have a special
    * format for the argument: column name string with prefix 'name' or index number with 'index' prefix,
    *
    *     e.g. "name:city_name" or "index:9"
    *
    * So right the evaluator be used, we need to remove the 'name' or 'index' prefix in the param value,
    * and if the prefix is 'index', we also need to replace the param value with actual column name at that index.
    *
    * @param dataFrame the DataFrame object
    * @param colResettable the Estimator/Evaluator object that needs to reset column params
    *
    * @return the code to extract and reset the column params of the estimator/evaluator passed in.
    */
  protected final def extractAndResetColParamsSnippet(dataFrame: String, colResettable: String): String = {
    s"""
       |val allColumns: Array[String] = $dataFrame.schema.fieldNames
       |
       |def extractColName(columnParam: String): String = columnParam.split(":") match {
       |  case Array(kind, value) =>
       |    if (kind == "name") value else if (kind == "index") allColumns(value.toInt) else value
       |  case _ => columnParam
       |}
       |
       |val colParamsToBeFormatted = Array(
       |  ("labelCol", "getLabelCol", "setLabelCol"),
       |  ("featuresCol", "getFeaturesCol", "setFeaturesCol"),
       |  ("censorCol", "getCensorCol", "setCensorCol"),
       |  ("predictionCol", "getPredictionCol", "setPredictionCol"),
       |  ("probabilityCol", "getProbabilityCol", "setProbabilityCol"),
       |  ("rawPredictionCol", "getRawPredictionCol", "setRawPredictionCol"),
       |  ("weightCol", "getWeightCol", "setWeightCol"),
       |  ("userCol", "getUserCol", "setUserCol"),
       |  ("itemCol", "getItemCol", "setItemCol"),
       |  ("ratingCol", "getRatingCol", "setRatingCol")
       |)
       |
       |colParamsToBeFormatted.foreach { case (paramName, getterName, setterName) =>
       |  if ($colResettable.hasParam(paramName)) {
       |    val p = $colResettable.params.find(_.name == paramName).get
       |    if ($colResettable.isSet(p)) {
       |      val getter = $colResettable.getClass.getMethod(getterName)
       |      val originValue: String = getter.invoke($colResettable).asInstanceOf[String]
       |      val setter = $colResettable.getClass.getMethod(setterName, classOf[String])
       |      setter.invoke($colResettable, extractColName(originValue))
       |    }
       |  }
       |}
     """.stripMargin
  }
}
