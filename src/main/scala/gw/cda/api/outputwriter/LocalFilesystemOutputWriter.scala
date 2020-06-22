package gw.cda.api.outputwriter

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import org.apache.commons.io.FileUtils

private[outputwriter] class LocalFilesystemOutputWriter(override val outputPath: String, override val includeColumnNames: Boolean,
                                                        override val saveAsSingleFile: Boolean, override val saveIntoTimestampDirectory: Boolean,
                                                        override val clientConfig: ClientConfig) extends OutputWriter {

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
