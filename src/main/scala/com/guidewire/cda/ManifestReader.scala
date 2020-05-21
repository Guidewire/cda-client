package com.guidewire.cda

import gw.cda.api.utils.ObjectMapperSupplier
import gw.cda.api.utils.S3ClientSupplier
import org.apache.logging.log4j.LogManager

private[cda] case class ManifestEntry(lastSuccessfulWriteTimestamp: String,
                                      totalProcessedRecordsCount: Int,
                                      dataFilesPath: String,
                                      schemaHistory: Map[String, String])

/*
  This class will fetch a manifest file from S3, and return it as a ManifestReader object.
  The manifest file has one ManifestEntry per table name, where each ManifestEntry has multiple attributes.
 */

object ManifestReader {

  private[cda] type ManifestMap = Map[String, ManifestEntry]
  private val log = LogManager.getLogger

  /** Function to first fetch manifest.json file from s3 as a string based on
   * parameters from config file, then use jackson to parse manifest JSON to a scala map
   *
   * @param bucketName is the name of S3 bucket where the manifest is stored
   * @param manifestKey is the path in S3 where the manifest is stored
   * @return
   */
  def processManifest(bucketName: String, manifestKey: String): ManifestReader.ManifestMap = {
    val manifestJSON: String = getManifestJson(bucketName, manifestKey)
    ManifestReader.parseManifestJson(manifestJSON)
  }

  /** Use jackson to parse manifest JSON to a scala map
   * Map keys are the names of each table e.g. "cctl_yesno"
   * Map values are another map that contains the data fields
   * from the json for each table
   *
   * @param manifestJson String contents of manifest file
   * @return Map of table names to maps of table data fields
   */
  private[cda] def parseManifestJson(manifestJson: String): ManifestReader.ManifestMap = {
    val parsedManifest = ObjectMapperSupplier.jsonMapper.readValue(manifestJson, classOf[Map[String, Any]])

    // Convert each entry in the manifest to a ManifestEntry class
    val manifest = parsedManifest.mapValues(ObjectMapperSupplier.jsonMapper.convertValue(_, classOf[ManifestEntry]))
    log.info(s"Successfully parsed manifest JSON file")
    manifest
  }

  /** Fetch manifest.json file from s3 as a string based on parameters from config file
   * Example entry from manifest.json
   * "taccount": {
   * "lastSuccessfulWriteTimestamp": "1562112543749",
   * "totalProcessedRecordsCount": 240000,
   * "dataFilesPath": "s3://cda-client-test/taccount
   * "schemaHistory": {
   *   "123456789": "1562112543749"
   * }
   *
   * @param bucketName is the name of S3 bucket where the manifest is stored
   * @param manifestKey is the path in S3 where the manifest is stored
   * @return Manifest contents as string
   */
  private[cda] def getManifestJson(bucketName: String, manifestKey: String): String = {
    val manifestJson = S3ClientSupplier.s3Client.getObjectAsString(bucketName, manifestKey)
    ManifestReader.log.info(s"Read manifest file from bucket: $bucketName with key: $manifestKey")
    manifestJson
  }

}
