package com.guidewire.cda

import java.nio.file.{Files, Paths}
import com.amazonaws.services.s3.AmazonS3URI
import com.guidewire.cda.config.ClientConfig
import com.guidewire.cda.config.ClientConfigReader
import com.guidewire.cda.specs.CDAClientTestSpec
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.io.Source
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TableReaderTest extends CDAClientTestSpec {

  def makeDummyDataFrame(letters: String, numbers: Range): DataFrame = {
    val stringList = letters.map(_.toString)
    val tempPairList = numbers.zip(stringList)
    val pairRdd = sparkSession.sparkContext.parallelize(tempPairList)
    val schema = StructType(List(
      StructField("c1", IntegerType, nullable = false),
      StructField("c2", StringType, nullable = false),
      StructField("c3", StringType, nullable = false)))
    val rowRdd = pairRdd.map(r => Row(r._1, r._2, r._2))
    sparkSession.createDataFrame(rowRdd, schema)
  }

  describe("Testing TableReader functionality") {
    val clientConfig: ClientConfig = ClientConfigReader.processConfigFile(testConfigPath)
    val tableReader = new TableReader(clientConfig)

    val testManifestSource = Source.fromFile(testManifestPath)
    val testManifestJson = testManifestSource.getLines.mkString("\n")
    testManifestSource.close

    val letters = "ABCDEFGHI"
    val numbers = 1 to 10

    val testTableName = "testTableName"

    val letters2 = "JKLMNOPQR"
    val numbers2 = 11 to 20

    val letters3 = "STUVWXYZ!"
    val numbers3 = 21 to 30

    describe("TableReader.getTableS3BaseLocationFromManifestEntry") {
      it("should get a base S3 URI from a manifest entry and table name") {
        val testManifest = ManifestReader.parseManifestJson(testManifestJson)
        val testBaseUri = tableReader.getTableS3BaseLocationFromManifestEntry("taccountlineitem", testManifest("taccountlineitem"))
        testBaseUri shouldEqual new AmazonS3URI("s3://cda-client-test/CDA/SQL/taccountlineitem/")
      }
    }

    describe("TableReader.filterTimestampSubfolders") {
      it("should filter out timestamp subfolders that are past that folder's lastSuccessfulWriteTimestamp") {
        val testManifest = ManifestReader.parseManifestJson(testManifestJson) //1562112543749
        val testTimestampSubfolderInfo1 = TableS3LocationWithTimestampInfo("taccountlineitem", schemaFingerprint = null, timestampSubfolderURI = null, "1562112543740".toLong)
        tableReader.isTableS3LocationWithTimestampSafeToCopy(testTimestampSubfolderInfo1, testManifest) shouldEqual true
        val testTimestampSubfolderInfo2 = TableS3LocationWithTimestampInfo("taccountlineitem", schemaFingerprint = null, timestampSubfolderURI = null, "1562112543760".toLong)
        tableReader.isTableS3LocationWithTimestampSafeToCopy(testTimestampSubfolderInfo2, testManifest) shouldEqual false
        val testTimestampSubfolderInfo3 = TableS3LocationWithTimestampInfo("taccountlineitem", schemaFingerprint = null, timestampSubfolderURI = null, "1562112543749".toLong)
        tableReader.isTableS3LocationWithTimestampSafeToCopy(testTimestampSubfolderInfo3, testManifest) shouldEqual true // should be inclusive of latest timestamp
      }
    }

    describe("TableReader.dropInternalColumns") {
      it("should drop irrelevant internal columns i.e. columns that begin with 'gwcbi___'") {
        val stringList = "ABCDEFGHI".map(_.toString)
        val pairRdd = sparkSession.sparkContext.parallelize(stringList)
        val schema = StructType(List(
          StructField("gwcbi___c1", StringType, nullable = false),
          StructField("gwcbi___c2", StringType, nullable = false),
          StructField("gwcbic3", StringType, nullable = false),
          StructField("GWCBI___c4", StringType, nullable = false),
          StructField("c5", StringType, nullable = false),
          StructField("c6", StringType, nullable = false)))
        val rowRdd = pairRdd.map(r => Row(r, r, r, r, r, r))
        val df = sparkSession.createDataFrame(rowRdd, schema)

        val dfNoInternal = tableReader.dropIrrelevantInternalColumns(df)
        dfNoInternal.columns.length shouldEqual 3
        noException should be thrownBy dfNoInternal.select("gwcbic3", "c5", "c6")
      }

      it("should not drop relevant internal columns") {
        val colValues = "ABCD".map(_.toString)
        val pairRdd = sparkSession.sparkContext.parallelize(colValues)
        val schema = StructType(List(
          StructField("gwcbi___seqval_hex", StringType, nullable = false),
          StructField("gwcbi___operation", StringType, nullable = false),
          StructField("gwcbi___something", StringType, nullable = false),
          StructField("not_internal_column", StringType, nullable = false)
        ))
        val rowRdd = pairRdd.map(r => Row(r, r, r, r))
        val df = sparkSession.createDataFrame(rowRdd, schema)

        val dfWithRelevantInternalColumns = tableReader.dropIrrelevantInternalColumns(df)
        dfWithRelevantInternalColumns.columns.length shouldEqual 3
        noException should be thrownBy dfWithRelevantInternalColumns.select("gwcbi___seqval_hex", "gwcbi___operation", "not_internal_column")
      }
    }

    describe("TableReader.reduceDataFrameWrappers") {
      it("should combine two TableDFInfo's into one, with their DataFrames unioned") {
        val testSchemaFingerprint = "schemaFingerprint"
        val testDF = makeDummyDataFrame(letters, numbers)
        val testTableDFInfo1 = DataFrameWrapper(testTableName, testSchemaFingerprint, testDF)

        val testDF2 = makeDummyDataFrame(letters2, numbers2)
        val testTableDFInfo2 = DataFrameWrapper(testTableName, testSchemaFingerprint, testDF2)

        val testTableDFFull = tableReader.reduceDataFrameWrappers(testTableDFInfo1, testTableDFInfo2)
        val testDFFull = testDF.union(testDF2)
        testTableDFFull shouldBe a[DataFrameWrapper]
        testTableDFFull.tableName shouldEqual testTableName
        testTableDFFull.schemaFingerprint shouldEqual testSchemaFingerprint
        testTableDFFull.dataFrame.collect should contain theSameElementsAs testDFFull.collect
      }
    }

    describe("TableReader.reduceTimestampSubfolderDataFrames") {
      it("should reduce a list of DataFrameWrapper into one single DataFrameWrappers") {
        val testSchemaFingerprint = "schemaFingerprint"
        val testDF1 = makeDummyDataFrame(letters, numbers)
        val testTableDFInfo1 = DataFrameWrapper(testTableName, testSchemaFingerprint, testDF1)

        val testDF2 = makeDummyDataFrame(letters2, numbers2)
        val testTableDFInfo2 = DataFrameWrapper(testTableName, testSchemaFingerprint, testDF2)

        val testDF3 = makeDummyDataFrame(letters3, numbers3)
        val testTableDFInfo3 = DataFrameWrapper(testTableName, testSchemaFingerprint, testDF3)

        val testTableDFInfoList = List(testTableDFInfo1, testTableDFInfo2, testTableDFInfo3)

        val testDFFull = testDF1.union(testDF2).union(testDF3)
        tableReader.reduceTimestampSubfolderDataFrames(testTableName, testTableDFInfoList).collect should contain theSameElementsAs testDFFull.collect
      }
    }

    describe("TableReader.getFingerprintsWithUnprocessedRecords") {
      val testManifest = ManifestReader.parseManifestJson(testManifestJson)

      it("should retain all fingerprints if no savepoint data exists") {
        val tempSavepointsDir = Files.createTempDirectory(Random.nextLong.toHexString)
        try {
          val emptySavepoints = new SavepointsProcessor(tempSavepointsDir.toUri)
          val testManifestEntry = testManifest("taccountlineitem")
          val fingerprints = tableReader.getFingerprintsWithUnprocessedRecords(testTableName, testManifestEntry, emptySavepoints)
          fingerprints.toSet shouldEqual Set("123456789")
        } finally {
          FileUtils.deleteDirectory(tempSavepointsDir.toFile)
        }
      }

      it("should discard fingerprints without records left to process") {
        val existingSavepointsDir = Paths.get("src/test/resources").toUri
        val testSavepointsProcessor = new SavepointsProcessor(existingSavepointsDir)
        val testTableName = "taccount"
        val testManifestEntry = testManifest(testTableName)

        val fingerprints = tableReader.getFingerprintsWithUnprocessedRecords(testTableName, testManifestEntry, testSavepointsProcessor)
        fingerprints.toSet shouldEqual Set("987654321", "8675309")
      }
    }
  }
}
