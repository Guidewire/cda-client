package gw.cda.api.outputwriter

import java.io.FileNotFoundException

import com.amazonaws.services.s3.AmazonS3URI
import com.guidewire.cda.DataFrameWrapperForMicroBatch
import gw.cda.api.utils.S3ClientSupplier
import org.apache.hadoop.fs.FileAlreadyExistsException

private[outputwriter] class S3OutputWriter(override val outputPath: String, override val includeColumnNames: Boolean,
                                           override val saveAsSingleFileCSV: Boolean, override val saveIntoTimestampDirectory: Boolean) extends OutputWriter {

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

}