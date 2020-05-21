package com.guidewire.cda

import com.guidewire.cda.specs.CDAClientTestSpec

import scala.io.Source

class ManifestReaderTest extends CDAClientTestSpec {

  describe("Testing ManifestReader functionality") {

    describe("ManifestReader.parseManifestJson") {

      val testManifestSource = Source.fromFile(testManifestPath)
      val testManifestJson = testManifestSource.getLines.mkString("\n")
      testManifestSource.close

      it("should read the correct data from a manifest JSON") {
        val testManifest = ManifestReader.parseManifestJson(testManifestJson)
        testManifest.size shouldEqual 6
        val tableData = testManifest("taccountlineitem")
        tableData.lastSuccessfulWriteTimestamp shouldEqual "1562112543749"
        tableData.totalProcessedRecordsCount shouldEqual 25000
        tableData.dataFilesPath shouldEqual "s3://cda-client-test/CDA/SQL/taccountlineitem"
      }
    }

  }

}
