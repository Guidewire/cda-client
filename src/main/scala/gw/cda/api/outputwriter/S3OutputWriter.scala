package gw.cda.api.outputwriter

import com.amazonaws.services.s3.AmazonS3URI
import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.utils.{S3ClientSupplier, UriUtils}
import org.apache.hadoop.fs.FileAlreadyExistsException

import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.Paths

private[outputwriter] class S3OutputWriter(override val outputPath: URI, override val includeColumnNames: Boolean,
                                           override val saveAsSingleFile: Boolean, override val saveIntoTimestampDirectory: Boolean,
                                           override val clientConfig: ClientConfig) extends FileBasedOutputWriter {

  val outputURI = new AmazonS3URI(outputPath)

  /**
   * Validate the outputPath, making sure that it exists/is a valid directory. If there is a problem, throw an exception.
   * In the case of S3 output, makes sure that the outputPath is in an existing S3 bucket and is also not an existing key to a S3 object.
   */
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
  override def getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): URI = {
    val pathPrefix = UriUtils.scheme(outputPath, "s3a")
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    basePathToFolder
  }

  override def getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): URI = {
    val pathPrefix = outputURI.getKey
    val basePathToFolder = getBasePathToFolder(new URI("s3a", null, pathPrefix, null), tableDataFrameWrapperForMicroBatch)
    val fullPathToSchema = basePathToFolder.resolve(new URI(null, null, schemaFileName, null))
    fullPathToSchema
  }

  override def writeSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableDF = tableDataFrameWrapperForMicroBatch.dataFrame
    val yamlString = makeSchemaYamlString(tableDF)
    val yamlPath = Paths.get(getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch)).toString
    S3ClientSupplier.s3Client.putObject(outputURI.getBucket, yamlPath, yamlString)
  }
}