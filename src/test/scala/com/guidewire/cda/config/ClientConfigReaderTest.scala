package com.guidewire.cda.config

import com.guidewire.cda.specs.CDAClientTestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientConfigReaderTest extends CDAClientTestSpec {

  describe("Testing ConfigReader functionality") {

    //Happy Path
    describe("ConfigReader.parseConfig") {

      val clientConfig = ClientConfigReader.processConfigFile(testConfigPath)

      it("should get config successfully") {
        clientConfig shouldBe a[ClientConfig]
      }

      it("config should have s3 url for manifest") {
        clientConfig.sourceLocation shouldBe a[SourceLocation]
        clientConfig.sourceLocation.bucketName shouldBe a[String]
        clientConfig.sourceLocation.manifestKey shouldEqual "dummy/key.json"
      }

      it("config should have url for csv output location") {
        clientConfig.outputLocation shouldBe a[OutputLocation]
        clientConfig.outputLocation.path shouldBe a[String]
      }

      it("config should have value for includeColumnNames") {
        clientConfig.outputSettings shouldBe a[OutputSettings]
        clientConfig.outputSettings.includeColumnNames shouldEqual true
      }

      it("config should have value for saveAsSingleFile") {
        clientConfig.outputSettings.saveAsSingleFile shouldEqual true
      }

      it("config should have value for saveIntoTimestampDirectory") {
        clientConfig.outputSettings.saveIntoTimestampDirectory shouldEqual false
      }

      it("config should have default value for numberOfJobsInParallelMaxCount") {
        clientConfig.performanceTuning.numberOfJobsInParallelMaxCount shouldEqual Runtime.getRuntime.availableProcessors
      }

      it("config should have default value for numberOfThreadsPerJob") {
        clientConfig.performanceTuning.numberOfThreadsPerJob should be > 0
      }

    }

    //Error cases
    describe("ConfigReader.validate") {
      it("should throw an error when a section parameter has a string that does not look like not a boolean") {
        (the[InvalidConfigParameterException] thrownBy ClientConfigReader.processConfigFile(testConfigPathInvalid1)).getMessage should include("error while parsing the config file")
      }

      it("should throw an error when a section parameter is blank in the config file") {
        (the[InvalidConfigParameterException] thrownBy ClientConfigReader.processConfigFile(testConfigPathInvalid2)).getMessage should include("parameter is missing")
      }

      it("should throw an error when a section is missing in the config file") {
        (the[MissingConfigParameterException] thrownBy ClientConfigReader.processConfigFile(testConfigPathInvalid3)).getMessage should include("section is missing")
      }

      it("should throw an error when a section parameter has a trailing slash in the output path dir") {
        (the[InvalidConfigParameterException] thrownBy ClientConfigReader.processConfigFile(testConfigPathInvalid4)).getMessage should include("parameter has an invalid value")
      }

      it("should throw an error when sparkTuning.driverMemory is a decimal number instead of an integer") {
        (the[InvalidConfigParameterException] thrownBy ClientConfigReader.processConfigFile(testConfigPathInvalid5)).getMessage should include ("parameter has an invalid value")
      }

    }

  }

}
