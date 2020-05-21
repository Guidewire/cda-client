package com.guidewire.cda.config

case class InvalidConfigParameterException(message: String, cause: Throwable) extends Exception(message, cause)

case class MissingConfigParameterException(message: String, cause: Throwable) extends Exception(message, cause)
