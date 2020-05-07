
package common

/**
  * Case class to fully represents a table/CSV/JSON field.
  * @param name the name of the field.
  * @param originName the original name of the field.
  * @param fieldType the type of the field.
  * @param datePattern the date pattern of the field if the field type is a data/timestamp.
  */
case class Field(name: String, originName: String, fieldType: String, datePattern: Option[String])

/**
  * Case class to simply represents a table/CSV/JSON field.
  * @param name the name of the field.
  * @param fieldType the type of the field.
  */
case class SimpleField(name: String, fieldType: String)
