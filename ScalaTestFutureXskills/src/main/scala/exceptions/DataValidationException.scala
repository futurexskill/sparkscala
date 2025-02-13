package exceptions

case class DataValidationException(message: String) extends Exception(message)
