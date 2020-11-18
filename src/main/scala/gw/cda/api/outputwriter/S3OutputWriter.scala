package gw.cda.api.outputwriter

import java.io.FileNotFoundException

import com.amazonaws.services.s3.AmazonS3URI
import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.utils.S3ClientSupplier
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

private[outputwriter] class S3OutputWriter(override val outputPath: String, override val includeColumnNames: Boolean,
                                           override val saveAsSingleFile: Boolean, override val saveIntoTimestampDirectory: Boolean,
                                           override val clientConfig: ClientConfig) extends OutputWriter {

  val outputURI = new AmazonS3URI(outputPath)

  override def validate(): Unit = {
    //Make sure the S3 bucket exists
    if (!S3ClientSupplier.s3Client.doesBucketExistV2(this.outputURI.getBucket)) {
      throw new FileNotFoundException(s"S3 bucket ${this.outputURI.getBucket} does not exist")
    }
    //Make sure the S3 outputPath is not an existing file, since we expect it to be a directory that we will write into
    if (S3ClientSupplier.s3Client.doesObjectExist(this.outputURI.getBucket, this.outputURI.getKey)) {
      throw new FileAlreadyExistsException(s"S3 output destination $outputPath already exists and is a file")
    }
  }

  /* In the case of S3 output, additionally replace "s3://" connector with "s3a://".
   This is necessary since Hadoop connects to s3 using its s3a client and does not recommend
   using a s3 connector. Writing with "s3://" does indeed result in some issues such
   as messy temporary files, and there are no such problems with using "s3a://"
   */
  override def getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String = {
    val pathPrefix = outputPath.replaceFirst("^s3", "s3a")
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    basePathToFolder
  }

  override def getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String = {
    val pathPrefix = outputURI.getKey
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    val fullPathToSchema = s"$basePathToFolder/$schemaFileName"
    fullPathToSchema
  }

  override def writeSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableDF = tableDataFrameWrapperForMicroBatch.dataFrame
    val yamlString = makeSchemaYamlString(tableDF)
    val yamlPath = getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch)
    S3ClientSupplier.s3Client.putObject(outputURI.getBucket, yamlPath, yamlString)
  }

  override def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableName = tableDataFrameWrapperForMicroBatch.tableName

    if (clientConfig.outputSettings.exportTarget=="file") {
      clientConfig.outputSettings.fileFormat.toLowerCase match {
        case "csv" =>
          log.info(s"Writing '$tableName' DataFrame as CSV to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
          this.writeCSV(tableDataFrameWrapperForMicroBatch)
          this.writeSchema(tableDataFrameWrapperForMicroBatch)
          log.info(s"Wrote '$tableName' DataFrame as CSV complete, with columns ${tableDataFrameWrapperForMicroBatch.dataFrame.columns.toList}")
        case "parquet" =>
          log.info(s"Writing '$tableName' DataFrame as PARQUET to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
          this.writeParquet(tableDataFrameWrapperForMicroBatch)
          log.info(s"Wrote '$tableName' DataFrame as PARQUET complete")
        case other => throw new Exception(s"Unknown output file format $other")
      }
    }
  }

  override def schemasAreConsistent(fileDataFrame: DataFrame, jdbcSchemaName: String, tableName: String, schemaFingerprint: String, url: String,
                           user: String, pswd: String, spark: SparkSession, jdbcWriteType: JdbcWriteType.Value): Boolean = true
}