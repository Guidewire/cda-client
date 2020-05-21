package com.guidewire.cda.cli

case class CLIArgumentParserException(message: String, cause: Throwable) extends Exception(message, cause)

case class MissingCLIArgumentException(message: String, cause: Throwable) extends Exception(message, cause)
