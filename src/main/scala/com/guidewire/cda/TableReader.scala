package com.guidewire.cda

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.guidewire.cda.ManifestReader.ManifestMap
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.outputwriter._
import gw.cda.api.outputwriter.OutputWriterConfig
import gw.cda.api.utils.S3ClientSupplier
import org.apache.commons.lang.time.StopWatch
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

private[cda] case class TableS3BaseLocationInfo(tableName: String,
                                                baseURI: AmazonS3URI)

private[cda] case class TableS3BaseLocationWithFingerprintsWithUnprocessedRecords(tableName: String,
                                                                                  baseURI: AmazonS3URI,
                                                                                  fingerprintsWithUnprocessedRecords: Iterable[String])

private[cda] case class TableS3LocationWithTimestampInfo(tableName: String,
                                                         schemaFingerprint: String,
                                                         timestampSubfolderURI: AmazonS3URI,
                                                         subfolderTimestamp: Long)

private[cda] case class DataFrameWrapper(tableName: String,
                                         schemaFingerprint: String,
                                         dataFrame: DataFrame)

case class DataFrameWrapperForMicroBatch(tableName: String,
                                         schemaFingerprint: String,
                                         schemaFingerprintTimestamp: String,
                                         manifestTimestamp: String,
                                         dataFrame: DataFrame)



class TableReader(clientConfig: ClientConfig) {
  private val log = LogManager.getLogger

  private[cda] val relevantInternalColumns = Set("gwcbi___seqval_hex", "gwcbi___operation")

  // https://spark.apache.org/docs/latest/configuration.html
  private[cda] val conf: SparkConf = new SparkConf()
  conf.setAppName("Cloud Data Access Client")
  private val numberOfExecutorThreads = if (clientConfig.performanceTuning.numberOfJobsInParallelMaxCount > Runtime.getRuntime.availableProcessors) {
    s"local[${clientConfig.performanceTuning.numberOfJobsInParallelMaxCount}]"
  } else {
    "local[*]"
  }
  conf.setMaster(numberOfExecutorThreads)
  Option(clientConfig.sparkTuning)
    .foreach(sparkTuning => {
      // By default, Spark limits the maximum result size to 1GB, which is usually too small.
      // Setting it to '0' allows for an unlimited result size.
      val maxResultSize = Option(sparkTuning.maxResultSize).getOrElse("0")
      conf.set("spark.driver.maxResultSize", maxResultSize)
      Option(sparkTuning.executorMemory).foreach(conf.set("spark.executor.memory", _))
      Option(sparkTuning.driverMemory).foreach(conf.set("spark.driver.memory", _))
    })

  private[cda] val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
  private[cda] val sc: SparkContext = spark.sparkContext

  sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
  sc.setLogLevel("ERROR")

  def run(singleTableName: String = ""): Unit = {

    //Measure the total time
    val completeRunForAllTablesStopWatch = new StopWatch()
    completeRunForAllTablesStopWatch.start()

    try {
      // Get savepoints of last read--if they exist--and validate the path to write the savepoints at
      val savepointsProcessor = new SavepointsProcessor(clientConfig.savepointsLocation.path)

      // Create OutputWriter and validate it
      val outputPath = clientConfig.outputLocation.path
      val includeColumnNames = clientConfig.outputSettings.includeColumnNames
      val saveAsSingleFile = clientConfig.outputSettings.saveAsSingleFile
      val saveIntoTimestampDirectory = clientConfig.outputSettings.saveIntoTimestampDirectory
      val outputWriterConfig = OutputWriterConfig(outputPath, includeColumnNames, saveAsSingleFile, saveIntoTimestampDirectory, clientConfig)
      val outputWriter = OutputWriter(outputWriterConfig)
      outputWriter.validate()

      // Reading manifestMap JSON from S3
      val bucketName = clientConfig.sourceLocation.bucketName
      val manifestKey = clientConfig.sourceLocation.manifestKey
      val manifestMap: ManifestMap = ManifestReader.processManifest(bucketName, manifestKey)
      var useManifestMap = manifestMap

      // Prepare user configs log msg
      var logMsg = (s"""Starting Cloud Data Access Client:
                       |Source bucket: ${clientConfig.sourceLocation.bucketName}
                       |Manifest has ${manifestMap.size} tables: ${manifestMap.keys.mkString(", ")}
                       |Writing tables to $outputPath
                       |Save into JDBC Raw: ${clientConfig.outputSettings.saveIntoJdbcRaw}
                       |Save into JDBC Merged: ${clientConfig.outputSettings.saveIntoJdbcMerged}
                       |Export Target: ${clientConfig.outputSettings.exportTarget}
                       |File Format: ${clientConfig.outputSettings.fileFormat}
                       |Save as single file: $saveAsSingleFile
                       |Save files into timestamp sub directory: $saveIntoTimestampDirectory
                       |Including column names in csv output: $includeColumnNames
                       |jdbcConnectionRaw.jdbcUrl: ${clientConfig.jdbcConnectionRaw.jdbcUrl}
                       |jdbcConnectionRaw.jdbcSchema: ${clientConfig.jdbcConnectionRaw.jdbcSchema}
                       |jdbcConnectionRaw.jdbcSaveMode: ${clientConfig.jdbcConnectionRaw.jdbcSaveMode}
                       |jdbcConnectionMerged.jdbcUrl: ${clientConfig.jdbcConnectionMerged.jdbcUrl}
                       |jdbcConnectionMerged.jdbcSchema: ${clientConfig.jdbcConnectionMerged.jdbcSchema}""".stripMargin)

      // Filter the manifestMap if this is run for specific tables.
      var tablesToInclude: String = Option(clientConfig.outputSettings.tablesToInclude).getOrElse("")
      //If command line argument is present, use it instead of yaml value.
      if (!singleTableName.isEmpty) {
        tablesToInclude = singleTableName
      }

      if (!tablesToInclude.isEmpty) {
        tablesToInclude = tablesToInclude.replace(" ", "")
        val tablesList = tablesToInclude.split(",")
        logMsg = logMsg + (s"""
                              |Including ONLY ${tablesList.size} table(s): $tablesToInclude""".stripMargin)
        useManifestMap = manifestMap.filterKeys(tablesList.contains)
      }

      // Log user configs info we prepared above
      log.info(logMsg)

      // Print out the common ForkJoinPool, so we are sure our job/table specific ForkJoinPool is different (we dont want to mess with the common pool)
      (1 to 1).par.foreach(_ => {
        val commonForkJoinPool = ForkJoinPool.commonPool()
        //Doing this is the parallel list, to see the actual thread id from the common pool
        log.info(s"Not using the common forkJoinPool = $commonForkJoinPool")
      })

      // Iterate over the manifestMap, and process each table
      log.info("Calculating all the files to fetch, based on the manifest timestamps; this might take a few seconds...")

      val copyJobs = useManifestMap
        .map({ case (tableName, manifestEntry) =>
          // For each entry in the manifestMap, map the table name to s3 url
          val baseUri = getTableS3BaseLocationFromManifestEntry(tableName, manifestEntry)
          val tmpFingerprintsWithUnprocessedRecords = getFingerprintsWithUnprocessedRecords(tableName, manifestEntry, savepointsProcessor)
          val fingerprintsWithUnprocessedRecords: Iterable[String] = {
            getFingerprintToProcess(clientConfig, tmpFingerprintsWithUnprocessedRecords, tableName, savepointsProcessor, baseUri)
          }
          TableS3BaseLocationWithFingerprintsWithUnprocessedRecords(tableName, baseUri, fingerprintsWithUnprocessedRecords)
        })
        // Map each table s3 url to all corresponding timestamp subfolder urls (with new data to copy)
        .flatMap(tableBaseLocWithUnprocessedRecords => getTableS3LocationWithTimestampsAfterLastSave(tableBaseLocWithUnprocessedRecords, savepointsProcessor))
        // Keep only timestamp subfolders that fall within the range we want to read/copy
        .filter(isTableS3LocationWithTimestampSafeToCopy(_, manifestMap))
        // Put all the timestampSubfolderLocations in a map by (table name, schema fingerprint)
        .groupBy(tableInfo => (tableInfo.tableName, tableInfo.schemaFingerprint))

      // Iterate over the manifestMap, and process each table
      log.info("Starting to fetch and process all tables in the manifest")

      // Setup the rate limiter
      val semaphore = new Semaphore(clientConfig.performanceTuning.numberOfJobsInParallelMaxCount)
      log.info(s"Using numberOfJobsInParallelMaxCount = ${clientConfig.performanceTuning.numberOfJobsInParallelMaxCount}")

      //Keep track of the table-fingerprint pairs as we complete them
      val totalNumberOfTableFingerprintPairs = copyJobs.size
      val completedNumberOfTableFingerprintPairs = new AtomicInteger(0)

      // Start the Copy Jobs, one by one, each in their own thread; while using the semaphore to limit concurrency
      copyJobs.foreach({ case ((tableName, schemaFingerprint), tableInfo) =>
        log.info(s"Copy job is pending for '$tableName' with fingerprint '$schemaFingerprint'")
        semaphore.acquire() // this will block

        // Define the job to run, using a closure
        val runnableJob = new Runnable {
          override def run(): Unit = {
            var completed = false

            try {
              log.info(s"Copy Job is starting for '$tableName' for fingerprint '$schemaFingerprint'")
              copyTableFingerprintPairJob(tableName, schemaFingerprint, copyJobs.get((tableName, schemaFingerprint)), useManifestMap, savepointsProcessor, outputWriter)
              completed = true
            }
            catch {
              case e: Throwable =>
                log.warn(s"Copy Job FAILED for '$tableName' for fingerprint '$schemaFingerprint': $e")
              //log.warn(e)
            }
            finally {
              if (completed) {
                log.info(s"Copy Job is complete for '$tableName' for fingerprint '$schemaFingerprint'; completed: ${completedNumberOfTableFingerprintPairs.incrementAndGet()} of $totalNumberOfTableFingerprintPairs tables")
              }
              semaphore.release()
            }
          }
        }

        // Start the job in its own thread, this will not block, so we will setup the next job immediately
        new Thread(runnableJob).start()
      })
      // The main thread must wait for all the runnableJobs to finish (this is easier than attempting a Thread.join)
      while (semaphore.availablePermits() != clientConfig.performanceTuning.numberOfJobsInParallelMaxCount) {
        Thread.sleep(1000)
      }
      log.info("All Copy Jobs have been completed")

    } finally {
      spark.close
    }

    //All done
    completeRunForAllTablesStopWatch.stop()
    log.info(s"All tables done, took ${(completeRunForAllTablesStopWatch.getTime / 1000).toString} seconds")
  }

  /**
   * Fetch the proper Fingerprint to processed based on available timestamp folders for JDBC writes.
   * Returns all available Fingerprints for file-only writes.
   *
   * @param clientConfig            clientConfig object.
   * @param fingerprintsAvailable   List of Fingerprints with unprocessed records.
   * @param tableName               tableName evaluated.
   * @param savepointsProcessor     Savepoints processor.
   * @param baseUri                 Amazon S3 URI.
   */
  private def getFingerprintToProcess(clientConfig: ClientConfig,
                                      fingerprintsAvailable: Iterable[String],
                                      tableName: String,
                                      savepointsProcessor: SavepointsProcessor,
                                      baseUri: AmazonS3URI): Iterable[String] = {
    if (clientConfig.outputSettings.exportTarget=="jdbc") {
      // Need to allow multiple fingerprints now that we're dynamically altering table definitions
      // HOWEVER we can't handle fingerprints in parallel for a given table since we're executing
      // ALTER TABLE statements against the tables as the parquet file schemas change
      // When saving to JDBC, we want to be sure we only return one fingerprint in each load to avoid schema conflict.
      val lastReadPoint = savepointsProcessor.getSavepoint(tableName)
      val numberOfFingerprintsAvailable = fingerprintsAvailable.toSeq.length
      if(numberOfFingerprintsAvailable==1) {
        fingerprintsAvailable
      } else {
        var fingerprintToReturn: Iterable[String] = None: Iterable[String]
        val firstFingerprintInList = fingerprintsAvailable.toSeq.get(0)
        val nextReadPointKey = if (lastReadPoint.isDefined) { s"${baseUri.getKey}$firstFingerprintInList/${lastReadPoint.get.toLong + 1}"} else { null }
        val listObjectsRequest = new ListObjectsRequest(baseUri.getBucket, s"${baseUri.getKey}$firstFingerprintInList/", nextReadPointKey, "/", null)
        val objectList = S3ClientSupplier.s3Client.listObjects(listObjectsRequest)
        val numberOfTimestampFoldersRemaining = objectList.getCommonPrefixes.size()
        if(numberOfTimestampFoldersRemaining>0) {
          fingerprintToReturn=fingerprintsAvailable.iterator.toIterable.filter(_==firstFingerprintInList)
        } else {
          val secondFingerprintInList = fingerprintsAvailable.toSeq.get(1)
          fingerprintToReturn=fingerprintsAvailable.iterator.toIterable.filter(_==secondFingerprintInList)
        }
        fingerprintToReturn
      }
    } else {
      fingerprintsAvailable
    }
  }
  /**
   * Fetch a table-fingerprint pair from S3 and write it.
   *
   * @param tableName                   Name of the table.
   * @param schemaFingerprint           Schema fingerprint for the pair.
   * @param timestampSubfolderLocations Locations of the timestamp subfolders to read from.
   * @param manifestMap                 Manifest entries.
   * @param savepointsProcessor         Savepoints processor.
   * @param outputWriter                Writer that controls where table is written.
   */
  private def copyTableFingerprintPairJob(tableName: String, schemaFingerprint: String,
                                          timestampSubfolderLocations: Option[Iterable[TableS3LocationWithTimestampInfo]],
                                          manifestMap: ManifestMap, savepointsProcessor: SavepointsProcessor,
                                          outputWriter: OutputWriter): Unit = {

    log.info(s"Processing '$tableName' with fingerprint '$schemaFingerprint', looking for new data, on thread ${Thread.currentThread()}")

    // Get the last timestamp in the fingerprint.  We will need this when we update savepoints later on.
    val maxTimestamp = if (timestampSubfolderLocations.isDefined) {
      timestampSubfolderLocations
        .get
        .maxBy(_.subfolderTimestamp)
        .subfolderTimestamp
    } else {
      0
    }

    log.info(s"maxTimestamp for fingerprint '$schemaFingerprint' = $maxTimestamp")
    // Read all timestamp subfolder urls, for this table, into DataFrames using Spark
    timestampSubfolderLocations
      .map(timestampSubfolderLocationsForTable => {
        // Start the StopWatch for this table
        log.info(s"New data found for '$tableName' with fingerprint '$schemaFingerprint'")
        val tableStopwatch = new StopWatch()
        tableStopwatch.start()

        // Setup the parallel threads to handle concurrent processing for this table/job
        val timestampSubfolderLocationsForTableParallel = timestampSubfolderLocationsForTable.par
        val forkJoinPool = new ForkJoinPool(clientConfig.performanceTuning.numberOfThreadsPerJob)
        timestampSubfolderLocationsForTableParallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
        log.info(s"The forkJoinPool for table '$tableName' with fingerprint '$schemaFingerprint' is setup = $forkJoinPool")

        //Fetch all the files in parallel
        val startReadTime = tableStopwatch.getTime
        val allDataFrameWrappersForTable: Iterable[DataFrameWrapper] = timestampSubfolderLocationsForTableParallel.map(fetchDataFrameForTableTimestampSubfolder).seq
        val fullReadTime = tableStopwatch.getTime - startReadTime
        log.info(s"Downloaded all data for fingerprint '$schemaFingerprint' for table '$tableName', took ${(fullReadTime / 1000.0).toString} seconds")

        // Combine all DataFrames for the table to one DataFrame
        val startReduceTime = tableStopwatch.getTime
        val dataFrameForTable: DataFrame = reduceTimestampSubfolderDataFrames(tableName, allDataFrameWrappersForTable)
        val fullReduceTime = tableStopwatch.getTime - startReduceTime
        log.info(s"Reduce DataFrames for table '$tableName' for fingerprint '$schemaFingerprint', took ${(fullReduceTime / 1000.0).toString} seconds")

        // Since all output types share a common savepoints.json,
        // make sure there are no schema change issues for JdbcRaw or JdbcMerged before writing data for
        // this fingerprint to any of the target types.
        val jdbcRawIsOk = if (clientConfig.outputSettings.exportTarget=="jdbc" && clientConfig.outputSettings.saveIntoJdbcRaw) {
          outputWriter.schemasAreConsistent(dataFrameForTable, clientConfig.jdbcConnectionRaw.jdbcSchema, tableName, schemaFingerprint,
            clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword, spark, outputWriter.JdbcWriteType.Raw)
        } else {
          true
        }

        val jdbcMergedIsOk = if (clientConfig.outputSettings.exportTarget=="jdbc" && clientConfig.outputSettings.saveIntoJdbcMerged) {
          outputWriter.schemasAreConsistent(dataFrameForTable, clientConfig.jdbcConnectionMerged.jdbcSchema, tableName, schemaFingerprint,
            clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword, spark, outputWriter.JdbcWriteType.Merged)
        } else {
          true
        }

        if (jdbcRawIsOk && jdbcMergedIsOk) {

          // Write each table and its schema to the configured output type(s) (CSV, Parquet, JDBC) to the configured location
          val startWriteTime = tableStopwatch.getTime
          val manifestTimestampForTable = manifestMap(tableName).lastSuccessfulWriteTimestamp
          val schemaFingerprintTimestamp = manifestMap(tableName).schemaHistory.getOrElse(schemaFingerprint, "unknown")
          val tableDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(tableName, schemaFingerprint, schemaFingerprintTimestamp,
            manifestTimestampForTable, dataFrameForTable)
          outputWriter.write(tableDataFrameWrapperForMicroBatch)
          val fullWriteTime = tableStopwatch.getTime - startWriteTime
          log.info(s"Wrote data for table '$tableName' for fingerprint '$schemaFingerprint', took ${(fullWriteTime / 1000.0).toString} seconds")

          // Get a list of all fingerprints that follow the one we are processing.
          val manifestEntry = manifestMap(tableName)
          val fingerprintsAfterCurrent = manifestEntry.schemaHistory
            .map({ case (schemaFingerprint, timestamp) => (schemaFingerprint, timestamp.toLong) })
            .filter({ case (_, timestamp) => timestamp > schemaFingerprintTimestamp.toLong })
            .toList
            .sortBy({ case (_, timestamp) => timestamp })

          // Log a warning message listing any additional fingerprints for this table
          // that are not being processed.
          if (clientConfig.outputSettings.exportTarget=="jdbc" && fingerprintsAfterCurrent.size > 0) {
            val bypassedFingerprintsList = fingerprintsAfterCurrent
              .map({ case (schemaFingerprint, _) => schemaFingerprint })
            log.warn(
              s"""
                 | $tableName fingerprint(s) were not processed in this load: ${bypassedFingerprintsList.toString.stripPrefix("List(").stripSuffix(")")}
                 |   Only one fingerprint per table can be processed at a time.""")
          }

          // If loading to jdbc target, only one Fingerprint will be processed at a time.
          // If loading files (CSV, Parquet), all Fingerprints are loaded.
          // Savepoints logic will be different for those scenarios.
          //    * If loading jdbc, use the last timestamp folder actually written.
          //    * If loading files, use the lastSuccessfulWriteTimestamp for that table from manifest.json.
          if(clientConfig.outputSettings.exportTarget=="jdbc") {
            savepointsProcessor.writeSavepoints(tableName, maxTimestamp.toString)
          } else {
            savepointsProcessor.writeSavepoints(tableName, manifestTimestampForTable)
          }
        }

        //Cleanup the forkJoinPool, to avoid a memory leak
        log.info(s"The forkJoinPool for table '$tableName' for fingerprint '$schemaFingerprint' is cleaning up = $forkJoinPool")
        forkJoinPool.shutdown()

        //Stop the StopWatch, and print out the results
        tableStopwatch.stop()
        log.info(s"Processed '$tableName' for fingerprint '$schemaFingerprint', took ${(tableStopwatch.getTime / 1000.0).toString} seconds")
      })
      .getOrElse(log.info(s"Skipping '$tableName' for fingerprint '$schemaFingerprint', no new data found"))
  }

  /**
   * Get the base S3 URI for a table from a manifest entry.
   *
   * @param tableName     The name of the table.
   * @param manifestEntry The manifest entry.
   * @return The base S3 URI for the table.
   */
  private[cda] def getTableS3BaseLocationFromManifestEntry(tableName: String, manifestEntry: ManifestEntry): AmazonS3URI = {
    val uriString = if (manifestEntry.dataFilesPath.endsWith("/")) {
      manifestEntry.dataFilesPath
    } else {
      manifestEntry.dataFilesPath + "/"
    }

    new AmazonS3URI(uriString)
  }

  /** Function to map a TableLocationInfo object to a list of TimestampSubfolderInfo objects.
   * "Timestamp subfolders" refer to the timestamped folders stored under the URI for each table,
   * each of which contain files for that table processed at that timestamp. Each TimestampSubfolderInfo
   * contains its corresponding table name, S3 URI, and parsed timestamp.
   *
   * @param tableInfo TableS3BaseLocationInfo containing table name and S3 URI
   * @return List[TableS3LocationWithTimestampInfo] containing timestamp subfolders' information for that table
   */
  private[cda] def getTableS3LocationWithTimestampsAfterLastSave(tableInfo: TableS3BaseLocationWithFingerprintsWithUnprocessedRecords, savepointsProcessor: SavepointsProcessor): Iterable[TableS3LocationWithTimestampInfo] = {
    val s3URI = tableInfo.baseURI
    val lastReadPoint = savepointsProcessor.getSavepoint(tableInfo.tableName)
    log.debug(s"Last read point timestamp for ${tableInfo.tableName} is $lastReadPoint")
    tableInfo.fingerprintsWithUnprocessedRecords.flatMap(fingerprint => {
      val nextReadPointKey = if (lastReadPoint.isDefined) { s"${s3URI.getKey}$fingerprint/${lastReadPoint.get.toLong + 1}"} else { null }
      val listObjectsRequest = new ListObjectsRequest(s3URI.getBucket, s"${s3URI.getKey}$fingerprint/", nextReadPointKey, "/", null)
      val objectList = S3ClientSupplier.s3Client.listObjects(listObjectsRequest)
      val timestampSubfolderKeys = objectList.getCommonPrefixes
      timestampSubfolderKeys.map(timestampSubfolderKey => {
        val timestampSubfolderURI = new AmazonS3URI(s"s3://${s3URI.getBucket}/$timestampSubfolderKey")
        val timestampPattern = ".+\\/([0-9]+)\\/$".r
        val timestampPattern(timestamp) = timestampSubfolderKey
        TableS3LocationWithTimestampInfo(tableInfo.tableName, fingerprint, timestampSubfolderURI, timestamp.toLong)
      })
    })
  }

  /** Determine if TableS3LocationWithTimestampInfo objects fall in the time window from which we want to read.
   * Currently, this function uses the lastSuccessfulWriteTimestamp in the manifest entry per table.
   *
   * @param tableTimestampSubfolderInfo TableS3LocationWithTimestampInfo corresponding to a timestamp subfolder for some table
   * @param manifest                    Manifest data (a map of string, TableManifestData) used to look up last successful write timestamps
   * @return Boolean if this timestamp subfolder should be read/copied
   */
  private[cda] def isTableS3LocationWithTimestampSafeToCopy(tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo, manifest: ManifestMap): Boolean = {
    val manifestTimestampForTable = manifest(tableTimestampSubfolderInfo.tableName).lastSuccessfulWriteTimestamp.toLong
    val includeFolder = tableTimestampSubfolderInfo.subfolderTimestamp <= manifestTimestampForTable
    if (!includeFolder) {
      log.debug(
        s"""Filtered out timestamp subfolder with timestamp ${tableTimestampSubfolderInfo.subfolderTimestamp} for table ${tableTimestampSubfolderInfo.tableName}
           |for being later than the manifest last successful write timestamp $manifestTimestampForTable""".stripMargin.replaceAll("\n", " ")
      )
    }
    includeFolder
  }

  /** Function to read each timestamp subfolder's files into a Spark DataFrame. Results will be stored in
   * a DataFrameWrapper object that contains the read DataFrame and the table name that it corresponds to.
   * It will also drop irrelevant internal columns from the DataFrame.
   *
   * @param tableTimestampSubfolderInfo TimestampSubfolderInfo corresponding to a timestamp subfolder for some table
   * @return DataFrameWrapper object that contains the DataFrame and table name
   */
  private[cda] def fetchDataFrameForTableTimestampSubfolder(tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo): DataFrameWrapper = {
    val s3URI = tableTimestampSubfolderInfo.timestampSubfolderURI
    val s3aURL = s"s3a://${s3URI.getBucket}/${s3URI.getKey}*"
    log.info(s"Reading '${tableTimestampSubfolderInfo.tableName}' from $s3aURL, on thread ${Thread.currentThread()}")
    // Adding in column renaming capabilities - we ran across the column name "interval", and cannot get the jdbc driver to stop creating the column with
    //   double quotes around it in the table definition. When we execute the Merged code, it thinks the value is a literal and won't insert.
    //   Adding additional .withColumnRenamed() lines below works in the event multiple columns need to be renamed.
    val dataFrame = spark.sqlContext.read.parquet(s3aURL)
      .withColumnRenamed("interval", "interval_")
    val dataFrameNoInternalColumns = dropIrrelevantInternalColumns(dataFrame)
    DataFrameWrapper(tableTimestampSubfolderInfo.tableName, tableTimestampSubfolderInfo.schemaFingerprint,
      dataFrameNoInternalColumns)
  }

  /** Function that takes a DataFrame corresponding to some table and drops internal columns that
   * are not necessary for the client.
   * Internal columns are currently defined as columns that begin with `gwcbi___`
   *
   * @param dataFrame DataFrame with internal columns
   * @return DataFrame that is the same as input, but has irrelevant internal columns removed
   */
  private[cda] def dropIrrelevantInternalColumns(dataFrame: DataFrame): DataFrame = {
    val dropList = dataFrame.columns.filter(colName => (colName.toLowerCase.startsWith("gwcbi___") && !relevantInternalColumns.contains(colName))
      // Get rid of geospatial columns.
      || colName.toLowerCase.contains("spatial")
      // Get rid of textdata columns. Contains XML data that won't insert into the database properly.
      || colName.toLowerCase.equals("textdata"))
    dataFrame.drop(dropList: _*)

  }

  /** Function that takes two DataFrameWrapper objects that correspond to the same table and returns a
   * DataFrameWrapper object that contains their two DataFrames unioned together, as well as keeping the
   * name.
   *
   * @param dataFrameWrapper1 DataFrameWrapper object that has a DataFrame for a table
   * @param dataFrameWrapper2 DataFrameWrapper object that has a DataFrame for the same table
   * @return DataFrameWrapper object that contains their two dataframes unioned together and the table name.
   */
  private[cda] def reduceDataFrameWrappers(dataFrameWrapper1: DataFrameWrapper, dataFrameWrapper2: DataFrameWrapper): DataFrameWrapper = {
    val dataFrame1 = dataFrameWrapper1.dataFrame
    val dataFrame2 = dataFrameWrapper2.dataFrame
    val combinedDataFrame = dataFrame1.unionByName(dataFrame2)

    //Make a new instance and return it as the unionized DataFrame
    dataFrameWrapper1.copy(dataFrame = combinedDataFrame)
  }

  /**
   * Combine multiple data frames that are part of the same table and have the same schema fingerprint into one.
   *
   * @param tableName            The name of the table.
   * @param dataFrameWrapperList The data frames to coalesce.
   * @return The combined data frame.
   */
  private[cda] def reduceTimestampSubfolderDataFrames(tableName: String, dataFrameWrapperList: Iterable[DataFrameWrapper]): DataFrame = {
    log.info(s"Reducing '$tableName' with ${dataFrameWrapperList.size} data frames")
    val tableCompleteDFInfo = dataFrameWrapperList.reduce(reduceDataFrameWrappers(_, _))
    log.info(s"Reduced '$tableName' complete, now it is a single data frame")
    tableCompleteDFInfo.dataFrame
  }

  /**
   * Get fingerprints with records not yet processed.
   *
   * @param tableName           Table to fetch fingerprints for.
   * @param manifestEntry       Manifest entry corresponding to the table.
   * @param savepointsProcessor Savepoint data processor.
   * @return Fingerprints for the table with records not yet processed.
   */
  private[cda] def getFingerprintsWithUnprocessedRecords(tableName: String, manifestEntry: ManifestEntry,
                                                         savepointsProcessor: SavepointsProcessor): Iterable[String] = {
    val lastProcessedTimestamp: Long = savepointsProcessor.getSavepoint(tableName).map(_.toLong).getOrElse(-1)
    val fingerprintsSortedByTimestamp = manifestEntry.schemaHistory
      .map({ case (schemaFingerprint, timestamp) => (schemaFingerprint, timestamp.toLong) })
      .toList
      .sortBy({ case (_, timestamp) => timestamp })


    val endOfTimeline = (fingerprintsSortedByTimestamp.last._1, Long.MaxValue)
    (fingerprintsSortedByTimestamp :+ endOfTimeline)
      .sliding(2)
      // Each fingerprint marks an interval in time. The start of the interval is
      // the timestamp in the fingerprint's manifest entry, and the end is the timestamp of the next,
      // or (effectively) infinity, if no such entry exists.
      .map({ case List((fingerprint, _), (_, schemaEndTimestamp)) => (fingerprint, schemaEndTimestamp) })
      // A fingerprint interval has unprocessed entries if we are inside it or it is further
      // along in time, i.e. the endpoint is further along than where we left off.
      .filter({ case (_, schemaEndTimestamp) => schemaEndTimestamp > lastProcessedTimestamp })
      .map({ case (fingerprint, _) => fingerprint })
      .toSeq
  }

}