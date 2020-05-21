package com.guidewire.cda.cli

import com.guidewire.cda.specs.CDAClientTestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CLIArgumentsProcessorTest extends CDAClientTestSpec {
  val configFileShortOption = "-c"
  val configFileOption = "--configPath"
  val configFileCompleteArg = "--configPath=some-config-file.yaml"
  val configFileName = "some-config-file.yaml"
  val unknownOption = "-g"
  val unknownValue = "guineaPig"

  describe("Testing CLIArguments functionality") {
    describe("CLIArguments.constructor") {
      it("should throw a MissingCLIArgumentException when the arguments provided are empty") {
        (the[MissingCLIArgumentException] thrownBy new CLIArgumentsProcessor(Seq())).getMessage should include("configPath")
      }

      it("should throw a CLIArgumentParserException when the arguments contain an option without a name") {
        an[CLIArgumentParserException] should be thrownBy new CLIArgumentsProcessor(Seq(configFileName))
      }

      it("should throw a CLIArgumentParserException when the arguments contain an option without a value") {
        (the[CLIArgumentParserException] thrownBy new CLIArgumentsProcessor(Seq(configFileOption))).getMessage should include("configPath")
      }

      it("should throw a CLIArgumentParserException when the arguments contain an unknown option") {
        an[CLIArgumentParserException] should be thrownBy new CLIArgumentsProcessor(Seq(configFileOption, configFileName, unknownOption, unknownValue))
      }

      it("should set the configFilePath when the arguments contain a config file option") {
        val cliArgumentsProcessor = new CLIArgumentsProcessor(Seq(configFileOption, configFileName))
        cliArgumentsProcessor.configFilePath should equal(configFileName)
      }

      it("should set the configFilePath when the arguments contain a config file short option") {
        val cliArgumentsProcessor = new CLIArgumentsProcessor(Seq(configFileShortOption, configFileName))
        cliArgumentsProcessor.configFilePath should equal(configFileName)
      }

      it("should set the configFilePath when the arguments contain a config file complete arg") {
        val cliArgumentsProcessor = new CLIArgumentsProcessor(Seq(configFileCompleteArg))
        cliArgumentsProcessor.configFilePath should equal(configFileName)
      }
    }
  }

}