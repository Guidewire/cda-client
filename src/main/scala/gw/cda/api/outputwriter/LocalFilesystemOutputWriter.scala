package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import org.apache.commons.io.FileUtils

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

private[outputwriter] class LocalFilesystemOutputWriter(override val outputPath: String, override val includeColumnNames: Boolean,
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

  override def getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String = {
    val pathPrefix = this.outputPath
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    basePathToFolder
  }

  override def getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String = {
    val pathPrefix = this.outputPath
    val basePathToFolder = getBasePathToFolder(pathPrefix, tableDataFrameWrapperForMicroBatch)
    val fullPathToSchema = s"$basePathToFolder/$schemaFileName"
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
