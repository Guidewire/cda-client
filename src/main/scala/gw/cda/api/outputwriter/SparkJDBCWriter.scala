package gw.cda.api.outputwriter

import com.guidewire.cda.config.ClientConfig
import com.guidewire.cda.DataFrameWrapperForMicroBatch
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.sql.DriverManager
import java.sql.SQLException

/**
 * This uses Spark's OOTB JDBC sink to write records to database.
 * This is going to use connection details from `jdbcV2Connection`
 * and write records to DB in raw format.
 */
private[outputwriter] class SparkJDBCWriter(override val clientConfig: ClientConfig) extends JdbcOutputWriter(clientConfig) {

  private val connectionConfig = clientConfig.jdbcV2Connection
  private val saveMode: SaveMode = connectionConfig.jdbcSaveMode match {
    case "overwrite" => SaveMode.Overwrite
    case "append"    => SaveMode.Append
    case other       =>
      log.warn(s"SaveMode = `$other` is not supported. Switching to 'append' mode.")
      SaveMode.Append
  }

  override def validate(): Unit = {
    super.validateDBConnection(connectionConfig.jdbcUrl, connectionConfig.jdbcUsername, connectionConfig.jdbcPassword)
  }

  override def schemasAreConsistent(dataframe: DataFrame, tableName: String,
                                    schemaFingerprint: String, sparkSession: SparkSession): Boolean = {

    schemasAreConsistent(dataframe, connectionConfig.jdbcSchema, tableName, schemaFingerprint,
      connectionConfig.jdbcUrl, connectionConfig.jdbcUsername,
      connectionConfig.jdbcPassword, sparkSession, JdbcWriteType.Raw)
  }

  override def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName
    val schemaFingerprint = tableDataFrameWrapperForMicroBatch.schemaFingerprint
    val tableNameWithSchema = s"${connectionConfig.jdbcSchema}.$tableNameNoSchema"
    val tableDataframe = tableDataFrameWrapperForMicroBatch.dataFrame.persist(StorageLevel.MEMORY_AND_DISK)
    val jdbcUrl = connectionConfig.jdbcUrl

    log.info(s"*** Writing '$tableNameNoSchema' raw data for fingerprint $schemaFingerprint as JDBC to $jdbcUrl")

    if (saveMode.equals(SaveMode.Append)) {
      val connection = DriverManager.getConnection(jdbcUrl, connectionConfig.jdbcUsername, connectionConfig.jdbcPassword)
      connection.setAutoCommit(false)
      val dbProductName = connection.getMetaData.getDatabaseProductName
      val tableNameCaseSensitive = dbProductName match {
        case "Microsoft SQL Server" | "PostgreSQL" => tableNameNoSchema
        case "Oracle"                              => tableNameNoSchema.toUpperCase
        case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
      }
      val tables = connection.getMetaData.getTables(connection.getCatalog, connectionConfig.jdbcSchema, tableNameCaseSensitive, Array("TABLE"))
      val tableExists = tables.next()
      val dialect = JdbcDialects.get(jdbcUrl)
      val insertSchema = tableDataframe.schema

      /* Create the table if it does not already exist. */
      if (!tableExists) {
        val createTableDDL = getTableCreateDDL(dialect, insertSchema, tableNameWithSchema, JdbcWriteType.Raw, dbProductName)
        // Execute the table create DDL
        val stmt = connection.createStatement
        log.info(s"Raw - $createTableDDL")
        stmt.execute(createTableDDL)
        stmt.close()
        super.createIndexes(connection, jdbcUrl, tableNameWithSchema, JdbcWriteType.Raw)
        connection.commit()
      }
    }

    writeDataframe(tableNameWithSchema, schemaFingerprint, tableDataframe, batchSize, JdbcWriteType.Raw)
    tableDataframe.unpersist()
    log.info(s"*** Finished writing '$tableNameNoSchema' raw data data for fingerprint $schemaFingerprint as JDBC to $jdbcUrl")
  }

  private def writeDataframe(tableName: String,
                             schemaFingerprint: String,
                             tableDataframe: DataFrame,
                             batchSize: Long,
                             jdbcWriteType: JdbcWriteType.Value): Unit = {
    try {
      tableDataframe.write
        .format("jdbc")
        .mode(saveMode)
        .option("driver", "org.postgresql.Driver")
        .option("url", connectionConfig.jdbcUrl)
        .option("dbtable", tableName)
        .option("user", connectionConfig.jdbcUsername)
        .option("password", connectionConfig.jdbcPassword)
        .save()

      log.info(s"$jdbcWriteType - All rows updated for table: $tableName and schema fingerprint: $schemaFingerprint")
    } catch {
      case e: Exception =>
        log.error(s"$jdbcWriteType - Update failed for table: $tableName and schema fingerprint: $schemaFingerprint.", e)
        throw e
    }
  }
}
