package com.guidewire.cda.specs

abstract class CDAClientTestSpec extends SparkSessionProviderTestSpec {
  val testManifestPath = "src/test/resources/sample-manifest.json"
  val testConfigPath = "src/test/resources/sample-config.yaml"
  val testConfigPathInvalid1 = "src/test/resources/sample-config-invalid-1.yaml"
  val testConfigPathInvalid2 = "src/test/resources/sample-config-invalid-2.yaml"
  val testConfigPathInvalid3 = "src/test/resources/sample-config-invalid-3.yaml"
  val testConfigPathInvalid4 = "src/test/resources/sample-config-invalid-4.yaml"
  val testConfigPathInvalid5 = "src/test/resources/sample-config-invalid-5.yaml"
}
