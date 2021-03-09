package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}

private[outputwriter] class KafkaOutputWriter (override val outputPath: String, override val includeColumnNames: Boolean,
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

  override def schemasAreConsistent(fileDataFrame: DataFrame, jdbcSchemaName: String, tableName: String, schemaFingerprint: String, url: String,
                                    user: String, pswd: String, spark: SparkSession, jdbcWriteType: JdbcWriteType.Value): Boolean = true

  override def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
      writeKafkaJson(tableDataFrameWrapperForMicroBatch)
  }

  def writeKafkaJson(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableDF = flattenDataframe(tableDataFrameWrapperForMicroBatch.dataFrame)
    val tableName = tableDataFrameWrapperForMicroBatch.tableName

    tableDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .mode("append")
      .option("kafka.bootstrap.servers", clientConfig.kafkaSettings.bootstrapServer)
      .option("kafka.acks", "all")
      .option("kafka.replication-factor", 2)
      .option("topic", tableName)
      .save()
  }

}
