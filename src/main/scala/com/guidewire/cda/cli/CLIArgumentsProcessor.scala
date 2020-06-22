package com.guidewire.cda.cli

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.exceptions.RequiredOptionNotFound
import org.rogach.scallop.exceptions.ScallopException

/*
  Class takes the command line arguments, and validates them for functional completeness
 */

class CLIArgumentsProcessor(args: Seq[String]) extends ScallopConf(args) with CLIArguments {

  // Define the possible input args, there is only 1 for now
  private val configFilePathScallopOption: ScallopOption[String] = opt(
    "configPath",
    'c',
    "Path to CDA Client configuration YAML file",
    required = true
  )

  private val singleTableValueScallopOption: ScallopOption[String] = opt(
    "tableName",
    't',
    "Selection of single table to pull data",
    default = Some(""),
    required = false
  )

  // Verify the input args are ok
  verify()

  // Make a public accessor to the actual value
  override val configFilePath: String = configFilePathScallopOption()
  override val singleTableValue: String = singleTableValueScallopOption() //SRhodes - 20200427

  // This will be called during the verify(), if there is a problem with the input args
  override def onError(e: Throwable): Unit = e match {
    case RequiredOptionNotFound(optionName) => throw MissingCLIArgumentException(optionName, e)
    case ScallopException(message)          => throw CLIArgumentParserException(message, e)
    case _                                  => super.onError(e)
  }
}
