package gw.cda.api.outputwriter

import java.io.StringWriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.utils.ObjectMapperSupplier
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, functions => sqlfun}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.util.Locale

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

import scala.util.parsing.json.JSONObject

import org.apache.spark.sql.SparkSession

case class OutputWriterConfig(outputPath: String, includeColumnNames: Boolean, saveAsSingleFile: Boolean, saveIntoTimestampDirectory: Boolean, clientConfig: ClientConfig)

trait OutputWriter {

  private val log = LogManager.getLogger
  val schemaFileName = "schema.yaml"

  //Instance vars that are passed in during constructor of the concrete classes
  val outputPath: String
  val includeColumnNames: Boolean
  val saveAsSingleFile: Boolean
  val saveIntoTimestampDirectory: Boolean
  val clientConfig: ClientConfig

  object JdbcWriteType extends Enumeration {
    type JdbcWriteType = Value

    val Raw = Value("Raw")
    val Merged = Value("Merged")
  }

  /** Validate the outputPath, making sure that it exists/is a valid directory.
   * If there is a problem, throw an exception.
   *
   * In the case of local output, makes sure that the outputPath is a directory
   * that exists and is not a file.
   *
   * In the case of S3 output, makes sure that the outputPath is in an existing
   * S3 bucket and is also not an existing key to a S3 object.
   *
   * @throws java.io.IOException If the validation was not successful
   *
   */
  def validate(): Unit

  /** Write a table and its schema to either local filesystem or to S3
   * and also to JDBC as indicated by clientConfig settings.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableName = tableDataFrameWrapperForMicroBatch.tableName

    // Process file write.
    if (clientConfig.outputSettings.saveIntoFile) {
      if (clientConfig.outputSettings.fileFormat.toLowerCase == "csv") {
        log.info(s"Writing '$tableName' DataFrame as CSV to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
        this.writeCSV(tableDataFrameWrapperForMicroBatch)
        this.writeSchema(tableDataFrameWrapperForMicroBatch)
        log.info(s"Wrote '$tableName' DataFrame as CSV complete, with columns ${tableDataFrameWrapperForMicroBatch.dataFrame.columns.toList}")
      } else { //parquet
        log.info(s"Writing '$tableName' DataFrame as PARQUET to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
        this.writeParquet(tableDataFrameWrapperForMicroBatch)
        log.info(s"Wrote '$tableName' DataFrame as PARQUET complete")
      }
    }

    if (clientConfig.outputSettings.saveIntoJdbcRaw && clientConfig.outputSettings.saveIntoJdbcMerged) {
      // If we are saving to both raw and merged datasets then we need to commit and rollback the
      // connections together since they share a common savevpoint.  If either fail then we need to
      // rollback both connections to keep them in sync.
      val rawConn = DriverManager.getConnection(clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword)
      rawConn.setAutoCommit(false) // Everything in the same db transaction.
      val mergedConn = DriverManager.getConnection(clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword)
      mergedConn.setAutoCommit(false)
      try {
        this.writeJdbcRaw(tableDataFrameWrapperForMicroBatch, rawConn)
      } catch {
        case e: Exception =>
          rawConn.rollback()
          rawConn.close()
          log.info(s"Raw - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
          mergedConn.close()
          throw e
      }
      try {
        this.writeJdbcMerged(tableDataFrameWrapperForMicroBatch, mergedConn)
      } catch {
        case e: Exception =>
          rawConn.rollback()
          rawConn.close()
          log.info(s"Raw - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
          mergedConn.rollback()
          mergedConn.close()
          log.info(s"Merged - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
          throw e
      }
      // Commit both connections.
      rawConn.commit()
      mergedConn.commit()
      // Close both connections.
      rawConn.close()
      mergedConn.close()
    } else {
      if (clientConfig.outputSettings.saveIntoJdbcRaw) {
        // Processing raw dataset only.
        val rawConn = DriverManager.getConnection(clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword)
        rawConn.setAutoCommit(false)
        try {
          this.writeJdbcRaw(tableDataFrameWrapperForMicroBatch, rawConn)
        } catch {
          case e: Exception =>
            rawConn.rollback()
            rawConn.close()
            log.info(s"Raw - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
            throw e
        }
        rawConn.commit()
        rawConn.close()
      } else {
        if (clientConfig.outputSettings.saveIntoJdbcMerged) {
          // Processing merged dataset only.
          val mergedConn = DriverManager.getConnection(clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword)
          mergedConn.setAutoCommit(false) // Everything in the same db transaction.
          try {
            this.writeJdbcMerged(tableDataFrameWrapperForMicroBatch, mergedConn)
          } catch {
            case e: Exception =>
              mergedConn.rollback()
              mergedConn.close()
              log.info(s"Merged - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
              throw e
          }
          mergedConn.commit()
          mergedConn.close()
        }
      }
    }
  }

  /** Write a table to a CSV file to either local filesystem or to S3.
   * Thanks to Spark, this is the same operation. Calls makeCSVPath to construct
   * the correct path location. Existing csv files in the same path are deleted
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def writeCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableDF = flattenDataframe(tableDataFrameWrapperForMicroBatch.dataFrame)
    val pathToFolderWithCSV = this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)

    if (saveAsSingleFile) {
      // This is much slower, coalesce(1) has to reshuffle all the partitions to 1
      tableDF.coalesce(1).write.option("header", includeColumnNames).mode(SaveMode.Overwrite).csv(pathToFolderWithCSV)
    } else {
      // This should be faster in theory, it writes 1 CSV per partition
      tableDF.write.option("header", includeColumnNames).mode(SaveMode.Overwrite).csv(pathToFolderWithCSV)
    }
  }

  /** Write a table to a Parquet.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def writeParquet(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val pathToFolderWithParquet = this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)
    if (saveAsSingleFile) {
      tableDataFrameWrapperForMicroBatch.dataFrame
        .coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .parquet(pathToFolderWithParquet)
    } else {
      tableDataFrameWrapperForMicroBatch.dataFrame.write
        .mode(SaveMode.Overwrite)
        .parquet(pathToFolderWithParquet)
    }
  }

  /** Write RAW data to a JDBC target database.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  private def writeJdbcRaw(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch, connection: Connection): Unit = {

    val tableName = clientConfig.jdbcConnectionRaw.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName // + "_" + tableDataFrameWrapperForMicroBatch.schemaFingerprintTimestamp
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName // + "_" + tableDataFrameWrapperForMicroBatch.schemaFingerprintTimestamp

    log.info(s"*** Writing '${tableDataFrameWrapperForMicroBatch.tableName}' raw data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")

    val InsertDF = tableDataFrameWrapperForMicroBatch.dataFrame
    InsertDF.cache()

    // Determine if we need to create the table by checking if the table already exists.
    val url = clientConfig.jdbcConnectionRaw.jdbcUrl
    val dbm = connection.getMetaData

    val tables = dbm.getTables(connection.getCatalog(), connection.getSchema(), tableNameNoSchema, Array("TABLE"))
    val tableExists = tables.next()

    // Get some data we will need for later.
    val dbProductName = dbm.getDatabaseProductName
    val dialect = JdbcDialects.get(url)
    val insertSchema = InsertDF.schema
    val batchSize = 5000 // consider making this configurable.

    // Create the table if it does not already exist.
    if (!tableExists) {
      // Build create table statement.
      val createTableDDL = getTableCreateDDL(dialect, insertSchema, tableName, JdbcWriteType.Raw, dbProductName)
      // Execute the table create DDL
      val stmt = connection.createStatement
      log.info(s"Raw - $createTableDDL")
      stmt.execute(createTableDDL)
      stmt.close()
      // Create table indexes for the new table.
      createIndexes(connection, url, tableName, JdbcWriteType.Raw)
      connection.commit()
    }

    // Build the insert statement.
    val columns = insertSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val placeholders = insertSchema.fields.map(_ => "?").mkString(",")
    val insertStatement = s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    log.info(s"Raw - $insertStatement")

    // Prepare and execute one insert statement per row in our insert dataframe.
    updateDataframe(connection, tableName, InsertDF, insertSchema, insertStatement, batchSize, dialect, JdbcWriteType.Raw)

    log.info(s"*** Finished writing '${tableDataFrameWrapperForMicroBatch.tableName}' raw data data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
  }

  /**
   * @param connection database connection
   * @param url used to determine db platform
   * @param tableName name of the table without the schema prefix
   * @param jdbcWriteType indicates Raw or Merged data write type
   */
  private def createIndexes(connection: Connection, url: String, tableName: String, jdbcWriteType: JdbcWriteType.Value): Unit = {
    val stmt = connection.createStatement
    val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
    if (url.toLowerCase.contains("sqlserver") || url.toLowerCase.contains("postgresql") || url.toLowerCase.contains("oracle")) {

      // Create primary key.
      var ddlPK = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + tableNameNoSchema + "_pk PRIMARY KEY "
      if (jdbcWriteType == JdbcWriteType.Merged) {
        ddlPK = ddlPK + "(\"id\")"
      }
      else {
        ddlPK = ddlPK + "(\"id\", \"gwcbi___seqval_hex\")"
      }
      log.info(s"$jdbcWriteType - $ddlPK")
      stmt.execute(ddlPK)

      // Create alternate keys for Merged data.  Raw data will not have any alternate keys since columns other than
      // the PK can be null (due to records for deletes).
      if (jdbcWriteType == JdbcWriteType.Merged) {
        var ddlAK1 = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + tableNameNoSchema + "_ak1 UNIQUE "
        if (tableNameNoSchema.startsWith("pctl_") || tableNameNoSchema.startsWith("cctl_") || tableNameNoSchema.startsWith("bctl_") || tableNameNoSchema.startsWith("abtl_")) {
          ddlAK1 = ddlAK1 + "(\"typecode\")"
        }
        else {
          ddlAK1 = ddlAK1 + "(\"publicid\")"
        }
        log.info(s"$jdbcWriteType - $ddlAK1")
        stmt.execute(ddlAK1)
      }

    } else {
      log.info(s"Unsupported database.  $url. Indexes were not created.")
      stmt.close()
      throw new SQLException(s"Unsupported database platform: $url")
     }

      stmt.close()
    }

  /** Merge the raw transactions into a JDBC target database applying the inserts/updates/deletes
   * according to transactions in the raw CDC data.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  private def writeJdbcMerged(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch, connection: Connection): Unit = {

    log.info(s"+++ Merging '${tableDataFrameWrapperForMicroBatch.tableName}' data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")

    val tableName = clientConfig.jdbcConnectionMerged.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName

    tableDataFrameWrapperForMicroBatch.dataFrame.cache()

    // Get list of CDA internal use columns to get rid of.
    val dropList = tableDataFrameWrapperForMicroBatch.dataFrame.columns.filter(colName => colName.toLowerCase.startsWith("gwcbi___"))

    // Log total rows to be merged for this fingerprint.
    val totCnt = tableDataFrameWrapperForMicroBatch.dataFrame.count()
    log.info(s"Merged - $tableName total cnt for all ins/upd/del: ${totCnt.toString}")

    // Filter for records to insert and drop unwanted columns.
    val InsertDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(2, 0))
      .drop(dropList: _*)
      .cache()

    // Log total rows to be inserted for this fingerprint.
    val insCnt = InsertDF.count()
    log.info(s"Merged - $tableName insert cnt after filter: ${insCnt.toString}")

    // Determine if we need to create the table by checking if the table already exists.
    val url = clientConfig.jdbcConnectionMerged.jdbcUrl
    val dbm = connection.getMetaData
    val tables = dbm.getTables(connection.getCatalog(), connection.getSchema(), tableNameNoSchema,  Array("TABLE"))
    val tableExists = tables.next

    // Get some data we will need for later.
    val dbProductName = dbm.getDatabaseProductName
    val dialect = JdbcDialects.get(url)
    val insertSchema = InsertDF.schema
    val batchSize = 5000 // consider making this configurable.

    // Create the table if it does not already exist.
    if (!tableExists) {
      // Build create table statement.
      val createTableDDL = getTableCreateDDL(dialect, insertSchema, tableName, JdbcWriteType.Merged, dbProductName)
      log.info(s"Merged - $createTableDDL")
      // Execute the table create DDL
      val stmt = connection.createStatement
      stmt.execute(createTableDDL)
      stmt.close()

      // Create table indexes for the new table.
      createIndexes(connection, url, tableName, JdbcWriteType.Merged)
      connection.commit()
    }

    // Build the insert statement.
    val columns = insertSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val placeholders = insertSchema.fields.map(_ => "?").mkString(",")
    val insertStatement = s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    log.info(s"Merged - $insertStatement")

    // Prepare and execute one insert statement per row in our insert dataframe.
    updateDataframe(connection, tableName, InsertDF, insertSchema, insertStatement, batchSize, dialect, JdbcWriteType.Merged)

    InsertDF.unpersist()

    // Filter for records to update.
    val UpdateDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(4))
      .cache()

    // Log total rows marked as updates.
    val UpdCnt = UpdateDF.count()
    log.info(s"Merged - $tableName update cnt after filter: ${UpdCnt.toString}")

    // Generate and apply update statements based on the latest transaction for each id.
    if (UpdCnt > 0) {

      // Get the list of columns
      val colNamesArray = UpdateDF.columns.toBuffer
      // Remove the id and sequence from the list of columns so we can handle them separately.
      // We will be grouping by the id and the sequence will be set as the first item in the list of columns.
      colNamesArray --= Array("id", "gwcbi___seqval_hex")
      val colNamesString = "gwcbi___seqval_hex, " + colNamesArray.mkString(",")

      val latestChangeForEachID = if (clientConfig.jdbcConnectionMerged.jdbcApplyLastestUpdatesOnly) {
        // Find the latest change for each id based on the gwcbi___seqval_hex.
        // Note: For nested structs, max on struct is computed as
        // max on first struct field, if equal fall back to second fields, and so on.
        // In this case the first struct field is gwcbi___seqval_hex which will be always
        // be unique for each instance of an id in the group.
        UpdateDF
          .selectExpr(Seq("id", s"struct($colNamesString) as otherCols"): _*)
          .groupBy("id").agg(sqlfun.max("otherCols").as("latest"))
          .selectExpr("latest.*", "id")
          .drop(dropList: _*)
          .cache()
      } else {
        // Retain all updates.  Sort so they are applied in the correct order.
        val colVar = colNamesString + ",id"
        UpdateDF
          .selectExpr(colVar.split(","): _*)
          .sort(col("gwcbi___seqval_hex").asc)
          .drop(dropList: _*)
          .cache()
      }
      UpdateDF.unpersist()

      val latestUpdCnt = latestChangeForEachID.count()
      if (clientConfig.jdbcConnectionMerged.jdbcApplyLastestUpdatesOnly) {
        // Log row count following the reduction to only last update for each id.
        log.info(s"Merged - $tableName update cnt after agg to get latest for each id: ${latestUpdCnt.toString}")
      } else {
        log.info(s"Merged - $tableName all updates will be applied in sequence.")
      }

      // Build the sql Update statement to be used as a prepared statement for the Updates.
      val colListForSetClause = latestChangeForEachID.columns.filter(_ != "id")
      val colNamesForSetClause = colListForSetClause.map(_ + " = ?").mkString(", ")
      val updateStatement = "UPDATE " + tableName + " SET " + colNamesForSetClause + " WHERE id = ?"
      log.info(s"Merged - $updateStatement")

      // Get schema info required for updatePartition call.
      val updateSchema = latestChangeForEachID.schema

      // Prepare and execute one update statement per row in our update dataframe.
      updateDataframe(connection, tableName, latestChangeForEachID, updateSchema, updateStatement, batchSize, dialect, JdbcWriteType.Merged)

      latestChangeForEachID.unpersist()
    }

    // Filter for records to be deleted.
    // Deletes should be relatively rare since most data is retired in InsuranceSuite rather than deleted.
    val DeleteDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(1))
      .selectExpr("id")
      .cache()

    // Log number of records to be deleted.
    val delCnt = DeleteDF.count()
    log.info(s"Merged - $tableName delete cnt after filter: ${delCnt.toString}")

    // Generate and apply delete statements.
    if (delCnt > 0) {
      val deleteSchema = DeleteDF.schema
      // Build the sql Delete statement to be used as a prepared statement for the Updates.
      val deleteStatement = "DELETE FROM " + tableName + " WHERE id = ?"
      log.info(s"Merged - $deleteStatement")

      // Prepare and execute one delete statement per row in our delete dataframe.
      updateDataframe(connection, tableName, DeleteDF, deleteSchema, deleteStatement, batchSize, dialect, JdbcWriteType.Merged)

      tableDataFrameWrapperForMicroBatch.dataFrame.unpersist()
      DeleteDF.unpersist()
    }
    log.info(s"+++ Finished merging '${tableDataFrameWrapperForMicroBatch.tableName}' data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
  }

  def getTableCreateDDL( dialect: JdbcDialect, schema: StructType, tableName: String, jdbcWriteType: JdbcWriteType.Value, dbProductName: String): String = {
    val sb = new StringBuilder()
    // Define specific columns we want to set as NOT NULL. Everything coming out of CDA parquet files is defined as nullable so
    // we do this to ensure there are columns available to set as PKs and/or AKs.
    var notNullCols = List("id", "gwcbi___operation", "gwcbi___seqval_hex")
    if (jdbcWriteType == JdbcWriteType.Merged) {
      // For merged data, include publicid in list of not null columns.
      notNullCols = notNullCols ++ List("publicid","retired","typecode")
    }
    // We will explicitly set the data type for string data to avoid nvarchar(max) and varchar2 types that are potentially too short.
    // nvarchar(max) columns can't be indexed.
    val stringDataType = dbProductName match {
      case "Microsoft SQL Server" => "VARCHAR(1333)"
      case "PostgreSQL" => "VARCHAR(1333)"
      case "Oracle" => "VARCHAR2(1333)"
      case _ => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    // Build the list of columns in alphabetic order.
    schema.fields.sortBy(f => f.name).foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = if (field.dataType == StringType) stringDataType else getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (notNullCols.contains(field.name) || !field.nullable) "NOT NULL" else ""
      sb.append(s"$name $typ $nullable, ")
    }
    // Remove the trailing comma.
    val colsForCreateDDL = sb.stripSuffix(", ")
    // Build and return the final create table statement.
    s"CREATE TABLE $tableName ($colsForCreateDDL)"
  }

  private def updateDataframe(conn: Connection,
                              table: String,
                              df: DataFrame,
                              rddSchema: StructType,
                              updateStmt: String,
                              batchSize: Int,
                              dialect: JdbcDialect,
                              jdbcWriteType: JdbcWriteType.Value
                             ): Unit = {
    var completed = false
    var totalRowCount = 0
    val dbProductName = conn.getMetaData.getDatabaseProductName
    try {
      val  stmt = conn.prepareStatement(updateStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      //For Oracle only - map nullTypes to TINYINT for Boolean to work around Oracle JDBC driver issues 
      val nullTypes = rddSchema.fields.map(f => if(dbProductName == "Oracle" && f.dataType == BooleanType) JdbcType("BYTE", java.sql.Types.TINYINT).jdbcNullType else getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0

        df.collect().foreach {row =>
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          log.info(s"${stmt.toString}")
          rowCount += 1
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            log.info(s"$jdbcWriteType - executeBatch - ${rowCount.toString} rows - $updateStmt")
            rowCount = 0
          }
        }

        if (rowCount > 0) {
          stmt.executeBatch()
          log.info(s"$jdbcWriteType - executeBatch - ${rowCount.toString} rows - $updateStmt")
        }
      } finally {
        stmt.close()
      }
      completed = true
    } catch {
      case e: SQLException =>
        //log.info(s"Catch exception for $table - $updateStmt")
        val cause = e.getCause
        val nextcause = e.getNextException
        if (nextcause != null && cause != nextcause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (cause == null) {
            try {
              e.initCause(nextcause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(nextcause)
            }
          } else {
            e.addSuppressed(nextcause)
          }
        }
        throw e
    } finally {
      if (!completed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through and tell the user about another problem.
          log.info(s"$jdbcWriteType - Update failed for $table - $updateStmt")
      } else {
        log.info(s"$jdbcWriteType - Total rows updated for $table: $totalRowCount rows - $updateStmt")
      }
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for $dt.catalogString"))
  }

  /**
   * Retrieve standard jdbc types.
   *
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  private def getCommonJDBCType(dt: DataType): Option[JdbcType] = {

    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  /**
   * A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
   * `PreparedStatement`. The last argument `Int` means the index for the value to be set
   * in the SQL statement and also used for the value in `Row`.
   * private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit
   */
  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  private def makeSetter(
                          conn: Connection,
                          dialect: JdbcDialect,
                          dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))
    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))
    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))
    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))
    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))
    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))
    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))
    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))
    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))
    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))
    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))
    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))
    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)
    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  /** Determine if table schema definition is the same as the parquet file schema definition.
   *
   * @param dataFrame based on the parquet file format
   * @param jdbcSchemaName database schema name
   * @param tableName is the name of the database table we being compared to
   * @param url database url
   * @param user database user name
   * @param pswd database password
   * @parm spark is the spark session.
   * @param jdbcWriteType Merge vs Raw to determine exclusion of internal 'gwcbi__' columns
   *                               when comparing schemas.  When merging data we remove those columns
   *                               from the data set before saving the data so we don't want to check
   *                               for them when comparing to the schema definition in the database.
   * @return Boolean indicating if the table schema definition is the same as the parquet file schema definition
   */
  def schmasAreConsistent(dataFrame: DataFrame, jdbcSchemaName: String, tableName: String, schemaFingerprint: String, url: String,
                          user: String, pswd: String, spark: SparkSession, jdbcWriteType: JdbcWriteType.Value): Boolean = {

    if (tableExists(tableName, url, user, pswd)) {
      //build a query that returns no data from the table.  This will still get us the schema definition which is all we need.
      val sql = "(select * from " + jdbcSchemaName + "." + tableName + " where 1=2) as " + tableName
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable",sql)
        .option("user", user)
        .option("password", pswd)
        .load()

      val dialect = JdbcDialects.get(url)

      // Derive the product name from the url to avoid having to create or pass in a connection
      // to access the metadata object.
      val dbProductName = if (url.toLowerCase.contains("sqlserver")) {
        "Microsoft SQL Server"}
      else { if (url.toLowerCase.contains("postgresql")) {
        "PostgreSQL"}  else {
        if (url.toLowerCase.contains("oracle")) {
        "Oracle"} }}

      // Generate the table create ddl statement based on the schema definition of the database table.
      val dbDDL = getTableCreateDDL(dialect, jdbcDF.schema, tableName, jdbcWriteType, dbProductName.toString)

      // Get the schema definition for the data read from the parquet file.
      val dfSchemaDef  = if (jdbcWriteType == JdbcWriteType.Merged) {
       val dropList = dataFrame.columns.filter(colName => colName.toLowerCase.startsWith("gwcbi___"))
       dataFrame.drop(dropList: _*).schema
      } else {
        dataFrame.schema
      }

      // Build the create ddl statement based on the data read from the parquet file.
      val dfDDL = getTableCreateDDL(dialect, dfSchemaDef, tableName, jdbcWriteType, dbProductName.toString)

      // Compare the two table definitions and log warnings if they do not match.
      if(dbDDL==dfDDL) {
        //log.info(s"File schema MATCHES Table schema")
        true
      } else {

        val logMsg = (s"""
  |
  | $jdbcWriteType table definition for '$tableName' does not match parquet fingerprint '$schemaFingerprint'.  Bypassing updates for fingerprint $schemaFingerprint.
  |
  | $tableName $jdbcWriteType DB Table Schema:
  | ${"-" * (tableName.length + jdbcWriteType.toString.length + 18)}
  | ${dbDDL.stripPrefix(s"CREATE TABLE $tableName (").stripSuffix(")")}
  |
  | $tableName Parquet Schema for Fingerprint $schemaFingerprint:
  | ${"-" * (tableName.length + schemaFingerprint.length + 33)}
  | ${dfDDL.stripPrefix(s"CREATE TABLE $tableName (").stripSuffix(")")}
  |""")
        log.warn(logMsg)
        false
      }
    }
    else {
      true
    }
  }

  def tableExists(tableName: String, url: String, user: String, pswd: String): Boolean = {
    val connection = DriverManager.getConnection(url, user, pswd)
    val dbm = connection.getMetaData
    val tables = dbm.getTables(connection.getCatalog(), connection.getSchema(), tableName, Array("TABLE"))
    if (tables.next) {
      connection.close()
      true
    } else {
      connection.close()
      false
    }

  }

  /**
    * Converts the nested column values into a string for type StructType
    */
  private def flattenDataframe(df: DataFrame): DataFrame = {

    df.schema.fields.foldLeft(df)((accumDf, field) => {
      field.dataType match {
        case _: StructType =>
          df.withColumn(field.name, stringifyRowUDF(df(field.name)))
        case _             => accumDf
      }
    })
  }

  private val stringifyRowUDF: UserDefinedFunction = udf[String, Row](parseColumn)

  /**
    * The method converts the BinaryType data back to the string
    */
  private def parseColumn: Row => String = {
    row: Row =>
      if (row == null)
        "null"
      else {
        val fieldMap = row.schema.fields.map { field =>
          field.dataType match {
            case _: BinaryType =>
              val index: Int = row.fieldIndex(field.name)
              val stringValue = row.get(index).asInstanceOf[Array[Byte]].map(_.toChar).mkString
              field.name -> stringValue
            case _             =>
              field.name -> row.getAs[String](field.name)
          }
        }.toMap
        JSONObject(fieldMap).toString()
      }
  }

  /** Constructs the correct path to local filesystem or to S3 location to write CSVs to.
    *
    * @param tableDataFrameWrapperForMicroBatch A DataFrameForMicroBatch with the table info
    * @return String with the correct path FOLDER to write the CSV
    */
  def getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String

  /** Constructs the correct path to local filesystem or to S3 location to write YAMLs to.
    *
    * @param tableDataFrameWrapperForMicroBatch A DataFrameForMicroBatch with the table info
    * @return String with the correct path FILE to write the SCHEMA
    */
  def getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String

  /** Build the path to a common folder structure: PREFIX/table/timestamp
    *
    * @param pathPrefix A DataFrame corresponding to a table
    * @param tableDataFrameWrapperForMicroBatch has the data to be written
    * @return the path to the folder
    */
  def getBasePathToFolder(pathPrefix: String, tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): String = {
    val pathWithTableName = s"$pathPrefix/${tableDataFrameWrapperForMicroBatch.tableName}/${tableDataFrameWrapperForMicroBatch.schemaFingerprint}"
    if (saveIntoTimestampDirectory) {
      s"$pathWithTableName/${tableDataFrameWrapperForMicroBatch.manifestTimestamp}"
    } else {
      pathWithTableName
    }
  }

  /** Write a table's schema to a .yaml file to either local filesystem or to S3. In both
    * cases, calling makeSchemaString on the table's DataFrame to collect the table's schema
    * into a yaml formatted string.
    *
    * In the case of local output, write string to schema file
    *
    * In the case of S3 output, upload string as an object to S3
    *
    * @param tableDataFrameWrapperForMicroBatch has the data to be written
    */
  def writeSchema(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit

  /** Collects a table's DataFrame's schema, pre-processes it , then writes it to a String
    * in Yaml format.
    *
    * @param tableDataFrame A DataFrame corresponding to a table
    * @return Yaml formatted string with the table's schema fields
    */
  def makeSchemaYamlString(tableDataFrame: DataFrame): String = {
    val schemaFieldsList = tableDataFrame.schema.fields.toList.map(field => {
      Map("name" -> field.name,
        "dataType" -> field.dataType.simpleString,
        "nullable" -> field.nullable)
    })
    val writer = new StringWriter()
    ObjectMapperSupplier.yamlMapper.writeValue(writer, schemaFieldsList)
    writer.toString
  }

}

object OutputWriter {

  // The apply() is like a builder, caller can create without using the 'new' keyword
  def apply(outputWriterConfig: OutputWriterConfig): OutputWriter = {
    if (outputWriterConfig.outputPath.startsWith("s3://"))
      new S3OutputWriter(outputWriterConfig.outputPath, outputWriterConfig.includeColumnNames, outputWriterConfig.saveAsSingleFile, outputWriterConfig.saveIntoTimestampDirectory, outputWriterConfig.clientConfig)
    else
      new LocalFilesystemOutputWriter(outputWriterConfig.outputPath, outputWriterConfig.includeColumnNames, outputWriterConfig.saveAsSingleFile, outputWriterConfig.saveIntoTimestampDirectory, outputWriterConfig.clientConfig)
  }

}
