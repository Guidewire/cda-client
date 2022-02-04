package com.guidewire.cda

import java.io.File
import java.io.IOException

import com.guidewire.cda.specs.CDAClientTestSpec
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class SavepointsProcessorTest extends CDAClientTestSpec {

  private val tempDir = System.getProperty("java.io.tmpdir").replaceAll("/?$", "/") //This will be an OS specific temp dir, with a "/" at the end
  private val testSavepointsPath: String = s"${tempDir}testsavepoints"
  private val testSavepointsDirectory = new File(testSavepointsPath)
  private val testSavepointsPathWithExistingFile: String = "src/test/resources"

  override def beforeAll(): Unit = {
    super.beforeAll()
    testSavepointsDirectory.mkdir()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      FileUtils.deleteDirectory(testSavepointsDirectory)
    }
  }

  describe("Testing SavepointsProcessor functionality") {

    describe("SavepointsProcessor.checkExists") {
      it("test when savepoints directory does not exist") {
        val testSavepointsPathDoesNotExist = "src/test/doesntexist"
        an[IOException] should be thrownBy new SavepointsProcessor(testSavepointsPathDoesNotExist)
      }

      it("test when savepoints directory exists, and check if the savepoints json file exists") {
        val testSavepointsProcessor2 = new SavepointsProcessor(testSavepointsPath)
        testSavepointsProcessor2.savepointsFileExists shouldEqual false

        val testSavepointsProcessor3 = new SavepointsProcessor(testSavepointsPathWithExistingFile)
        testSavepointsProcessor3.savepointsFileExists shouldEqual true
      }
    }

    describe("SavepointsProcessor.readSavepoints") {
      it("should read savepoints from a savepoints.json file in a given directory, or return empty Map if no savepoints file exist") {
        val testSavepointsProcessor1 = new SavepointsProcessor(testSavepointsPath)
        testSavepointsProcessor1.savepointsDataMap shouldEqual Map.empty

        val testSavepointsProcessor2 = new SavepointsProcessor(testSavepointsPathWithExistingFile)
        testSavepointsProcessor2.savepointsDataMap shouldBe a[scala.collection.mutable.Map[String, _]]
        testSavepointsProcessor2.savepointsDataMap("taccounttransaction") shouldBe a[String]
        testSavepointsProcessor2.savepointsDataMap("taccounttransaction") shouldEqual "1562112543749"
      }
    }

    describe("SavepointsProcessor.getSavepoints") {
      it("should get a savepoint for a table from a savepoints.json file, which is an Option") {
        val testSavepointsProcessor1 = new SavepointsProcessor(testSavepointsPath)
        testSavepointsProcessor1.getSavepoint("taccounttransaction") shouldEqual None

        val testSavepointsPath2 = "src/test/resources"
        val testSavepointsProcessor2 = new SavepointsProcessor(testSavepointsPath2)
        testSavepointsProcessor2.getSavepoint("taccounttransaction") shouldEqual Some("1562112543749")
      }
    }

    describe("SavepointsProcessor.writeSavepoints") {
      it("should write the lastSuccessfulReadTimestamps to a savepoints.json file") {
        // Get the last manifest written by CDA
        val testManifestSource = Source.fromFile(testManifestPath)
        val testManifestJson = testManifestSource.getLines.mkString("\n")
        testManifestSource.close
        val testManifest = ManifestReader.parseManifestJson(testManifestJson)

        // Update the savepoints file, based on the manifest, one table at a time
        val testSavepointsProcessor1 = new SavepointsProcessor(testSavepointsPath)
        testManifest.foreach(manifestEntry => {
          val tableName = manifestEntry._1
          val manifestEntryDetail = manifestEntry._2
          testSavepointsProcessor1.writeSavepoints(tableName, manifestEntryDetail.lastSuccessfulWriteTimestamp)
        })

        // Reload the savepoints file, and verify it is the same as the original manifest data
        val testSavepointsProcessor2 = new SavepointsProcessor(testSavepointsPath)
        val reReadTimestamps = testSavepointsProcessor2.savepointsDataMap
        val manifestTimestamps = testManifest.mapValues(_.lastSuccessfulWriteTimestamp)
        reReadTimestamps shouldEqual manifestTimestamps
      }

      it("should allow savepoint entry to be updated multiple times, for the same table") {
        // Get the last manifest written by CDA
        val testSavepointsProcessor1 = new SavepointsProcessor(testSavepointsPath)
        testSavepointsProcessor1.writeSavepoints("testTableNameA", "123")

        // Reload the savepoints file, and verify it is the same as the original manifest data
        val testSavepointsProcessor2 = new SavepointsProcessor(testSavepointsPath)
        val reReadTimestamps2 = testSavepointsProcessor2.savepointsDataMap
        reReadTimestamps2("testTableNameA") shouldEqual "123"

        //Update it, and re-read it again
        val testSavepointsProcessor3 = new SavepointsProcessor(testSavepointsPath)
        testSavepointsProcessor3.writeSavepoints("testTableNameA", "456")
        testSavepointsProcessor3.writeSavepoints("testTableNameB", "789")
        val reReadTimestamps3 = testSavepointsProcessor3.savepointsDataMap
        reReadTimestamps3("testTableNameA") shouldEqual "456"
        reReadTimestamps3("testTableNameB") shouldEqual "789"
      }
    }

  }

}
