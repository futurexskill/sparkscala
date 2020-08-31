package exceptions

case class InvalidEnvironmentException(private val message: String = "",
                                      private val cause: Throwable = None.orNull)
extends Exception(message,cause)