package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.net.URI

case class OutputWriterConfig(outputUri: URI, includeColumnNames: Boolean, saveAsSingleFile: Boolean, saveIntoTimestampDirectory: Boolean, clientConfig: ClientConfig)

trait OutputWriter {

  private[cda] val log = LogManager.getLogger
  val clientConfig: ClientConfig

  /**
   * Validate the Writer configuration.
   */
  def validate(): Unit

  /** Write a table and its schema to either local filesystem or to S3
   * and also to JDBC as indicated by clientConfig settings.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit

  /** Determine if table schema definition is the same as the parquet file schema definition.
   * If differences in columns, ADD or DROP necessary columns from database table to align definitions, and re-check
   * for a match between database and file schema definitions.
   *
   * @param dataframe         based on the parquet file format
   * @param tableName         is the name of the database table we being compared to
   * @param schemaFingerprint is the schema Id of the table schema
   * @param sparkSession      is the spark session.
   * @return Boolean indicating if the table schema definition is the same as the parquet file schema definition
   */
  def schemasAreConsistent(dataframe: DataFrame, tableName: String,
                           schemaFingerprint: String, sparkSession: SparkSession): Boolean = true
}

object OutputWriter {
  // The apply() is like a builder, caller can create without using the 'new' keyword
  def apply(outputWriterConfig: OutputWriterConfig): OutputWriter = {
    val targetType: String = outputWriterConfig.clientConfig.outputSettings.exportTarget
    targetType match {
      case "file"    => FileBasedOutputWriter(outputWriterConfig)
      case "jdbc"    => new JdbcOutputWriter(outputWriterConfig.clientConfig)
      case "jdbc_v2" => new SparkJDBCWriter(outputWriterConfig.clientConfig)
      case _         => throw new UnsupportedOperationException(s"Target type `$targetType` is not supported.")
    }
  }
}
