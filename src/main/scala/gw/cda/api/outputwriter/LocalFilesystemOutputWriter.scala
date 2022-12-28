package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.utils.UriUtils
import org.apache.commons.io.FileUtils

import java.io.File
import java.io.IOException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths

private[outputwriter] class LocalFilesystemOutputWriter(override val outputPath: URI, override val includeColumnNames: Boolean,
                                                        override val saveAsSingleFile: Boolean, override val saveIntoTimestampDirectory: Boolean,
                                                        override val clientConfig: ClientConfig) extends FileBasedOutputWriter {

  /**
   * Validate the outputPath, making sure that it exists/is a valid directory. If there is a problem, throw an exception.
   * In the case of local output, makes sure that the outputPath is a directory that exists and is not a file.
   */
  override def validate(): Unit = {
    if (!Files.isDirectory(Paths.get(outputPath))) {
      throw new IOException(s"$outputPath is either not a local directory or doesn't exist")
    }
  }

  override def getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): URI = {
    val pathPrefix = this.outputPath
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    basePathToFolder
  }

  override def getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): URI = {
    val pathPrefix = this.outputPath
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    val fullPathToSchema = UriUtils.append(basePathToFolder, Paths.get(schemaFileName))
    fullPathToSchema
  }

  override def writeSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableDF = tableDataFrameWrapperForMicroBatch.dataFrame
    val yamlString = makeSchemaYamlString(tableDF)
    val yamlPath = getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch)
    val yamlFile = new File(yamlPath)
    FileUtils.writeStringToFile(yamlFile, yamlString, null: String)
  }
}
