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

    // Process JdbcRaw write.
    if (clientConfig.outputSettings.saveIntoJdbcRaw) {
      log.info(s"***Writing '$tableName' raw data as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
      this.writeJdbcRaw(tableDataFrameWrapperForMicroBatch)
      log.info(s"***Wrote '$tableName' raw data as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
    }

    // Process JdbcMerged write.
    if (clientConfig.outputSettings.saveIntoJdbcMerged) {
      log.info(s"+++Merging '$tableName' data as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
      this.writeJdbcMerged(tableDataFrameWrapperForMicroBatch)
      log.info(s"+++Merged '$tableName' data as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
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
  private def writeJdbcRaw(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {

    val tableName = clientConfig.jdbcConnectionRaw.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName // + "_" + tableDataFrameWrapperForMicroBatch.schemaFingerprintTimestamp

    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName

    // Determine if we need to create indexes by checking if the table already exists.
    val connection = DriverManager.getConnection(clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword)
    val dbm = connection.getMetaData
    val tables = dbm.getTables(null, null, tableName, null)
    val needsIndexes = !tables.next

    // Insert the records and create the table if it does not exist.
    tableDataFrameWrapperForMicroBatch.dataFrame.write
      .format("jdbc")
      .mode(clientConfig.jdbcConnectionRaw.jdbcSaveMode.toLowerCase)
      .option("url", clientConfig.jdbcConnectionRaw.jdbcUrl)
      .option("dbtable", tableName)
      .option("user", clientConfig.jdbcConnectionRaw.jdbcUsername)
      .option("password", clientConfig.jdbcConnectionRaw.jdbcPassword)
      .save()

    // Create indexes for table PKs and AKs. Need case logic for different DBs.
    if (needsIndexes) {
      createIndexes(connection, clientConfig.jdbcConnectionRaw.jdbcUrl, tableNameNoSchema)
    }

    connection.close()
  }

  /**
   *
   * @param connection database connection
   * @param url used to determine db platform
   * @param tableName name of the table without the schema prefix
   */
  private def createIndexes(connection: Connection, url: String, tableName: String): Unit = {
    val stmt = connection.createStatement

    if (url.toLowerCase.contains("sqlserver")) {

      // We can't create unique constraints because all columns are nullable by CDA definition.
      //We will need to add another param to distinguish between Raw and Merged data once we are able to create PKs.  They will be different for each.
      //val ddlPk = "alter table " + tableNameNoSchema + " add constraint " + tableNameNoSchema + "_pk primary key nonclustered (gwcbi___seqval_hex)"
      //log.info(ddlPk)
      //stmt.execute(ddlPk)
      val ddlIdx1 = "create nonclustered index " + tableName + "_idx1 on " + tableName + " (id)"
      log.info(ddlIdx1)
      stmt.execute(ddlIdx1)
      // Not able able to index nvarchar(max) columns on sql server.
      /**
      val ddlIdx2 =
        if (tableName contains "pctl_") {
        "create nonclustered index " + tableNameNoSchema + "_idx2 on " + tableNameNoSchema + " (typecode)"
      } else {
        "create nonclustered index " + tableNameNoSchema + "_idx2 on " + tableNameNoSchema + " (publicid)"
      }
      log.info(ddlIdx2)
      stmt.execute(ddlIdx2)
       */

    } else {
      if (url.toLowerCase.contains("postgressql")) {
        // We can't create unique constraints because all columns are nullable by CDA definition.
        //We will need to add another param to distinguish between Raw and Merged data once we are able to create PKs.  They will be different for each.
        //val ddlPk = "create unique index " + tableName + "_pk on " + tableName + " (gwcbi___seqval_hex)"
        //log.info(ddlPk)
        //stmt.execute(ddlPk)
        val ddlIdx1 = "create  index " + tableName + "_idx1 on " + tableName + " (id)"
        log.info(ddlIdx1)
        stmt.execute(ddlIdx1)
        // Not able able to index nvarchar(max) columns on sql server.
        /**
        val ddlIdx2 =
        if (tableName contains "pctl_") {
        "create  index " + tableName + "_idx1 on " + tableName + " (typecode)"
      } else {
        "create  index " + tableName + "_idx1 on " + tableName + " (publicid)"
      }
      log.info(ddlIdx2)
      stmt.execute(ddlIdx2)
         */
      } else {
        if (url.toLowerCase.contains("oracle")) {
          // We can't create unique constraints because all columns are nullable by CDA definition.
          //We will need to add another param to distinguish between Raw and Merged data once we are able to create PKs.  They will be different for each.
          //val ddlPk = "create unique index " + tableName + "_pk on " + tableName + "(ID asc) nologging parallel"
          //log.info(ddlPk)
          //stmt.execute(ddlPk)
          val ddlIdx1 = "create index " + tableName + "_idx1 on " + tableName + "(ID asc) nologging parallel"
          log.info(ddlIdx1)
          stmt.execute(ddlIdx1)
          // Need to see if we can do this in oracle....not sure of the datatypes.
          /**
          val ddlIdx2 =
        if (tableName contains "pctl_") {
        "create index " + tableName + "_idx2 on " + tableName + " (typecode asc) nologging parallel"
      } else {
         "create index " + tableName + "_idx2 on " + tableName + " (publicid asc) nologging parallel"
      }
      log.info(ddlIdx2)
      stmt.execute(ddlIdx2)
           */
        } else
          log.info(s"Unsupported database.  $url. Indexes were not created.")
      }
      stmt.close()
    }
  }

  /** Merge the raw transactions into a JDBC target database applying the inserts/updates/deletes
   *  according to transactions in the raw CDC data.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  private def writeJdbcMerged(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {

    val tableName = clientConfig.jdbcConnectionMerged.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName

    // Get list of CDA internal use columns to get rid of.
    val dropList = tableDataFrameWrapperForMicroBatch.dataFrame.columns.filter(colName => colName.toLowerCase.startsWith("gwcbi___"))

    // Log total rows to be merged for this fingerprint.
    val totCnt = tableDataFrameWrapperForMicroBatch.dataFrame.count()
    log.info(s"$tableName total cnt for ins/upd/del: ${totCnt.toString}")

    // Filter for records to insert and drop unwanted columns.
    val InsertDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(2, 0))
      .drop(dropList: _*)

    // Log total rows to be inserted for this fingerprint.
    val insCnt = InsertDF.count()
    log.info(s"$tableName insert cnt after filter: ${insCnt.toString}")

    // Determine if we need to create indexes by checking if the table already exists.
    val connection = DriverManager.getConnection(clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword)
    val dbm = connection.getMetaData
    val tables = dbm.getTables(null, null, tableName, null)
    val needsIndexes = !tables.next

    // Insert the records.
    InsertDF.write
      .format("jdbc")
      .mode("append")
      .option("url", clientConfig.jdbcConnectionMerged.jdbcUrl)
      .option("dbtable", tableName)
      .option("user", clientConfig.jdbcConnectionMerged.jdbcUsername)
      .option("password", clientConfig.jdbcConnectionMerged.jdbcPassword)
      .save()

    InsertDF.unpersist(true)

    // Create indexes for table PKs and AKs. Need case logic for different DBs.
    if (needsIndexes) {
      createIndexes(connection, clientConfig.jdbcConnectionMerged.jdbcUrl, tableNameNoSchema)
    }

    // Filter for records to update.
    val UpdateDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(4))

    // Log total rows marked as updates.
    val UpdCnt = UpdateDF.count()
    log.info(s"$tableName update cnt after filter: ${UpdCnt.toString}")

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
          .coalesce(1) // multiple partitions can cause deadlocks.  need to look into different isolation levels maybe.
      } else {
        // Retain all updates.  Sort so they are applied in the correct order.
        val colVar = colNamesString + ",id"
        UpdateDF
          .selectExpr(colVar.split(","): _*)
          .sort(col("gwcbi___seqval_hex").asc)
          .drop(dropList: _*)
          .coalesce(1) // multiple partitions can cause deadlocks.  need to look into different isolation levels maybe.
      }

      val latestUpdCnt = latestChangeForEachID.count()
      if (clientConfig.jdbcConnectionMerged.jdbcApplyLastestUpdatesOnly) {
        // Log row count following the reduction to only last update for each id.
        log.info(s"$tableName update cnt after agg to get latest for each id: ${latestUpdCnt.toString}")
      } else {
        log.info(s"$tableName applying all updates in order: ${latestUpdCnt.toString}")
      }
      UpdateDF.unpersist()

      // Build the sql Update statement to be used as a prepared statement for the Updates.
      val colListForSetClause = latestChangeForEachID.columns.filter(_ != "id")
      val colNamesForSetClause = colListForSetClause.map(_ + " = ?").mkString(", ")
      val updateStatement = "UPDATE " + tableName + " SET " + colNamesForSetClause + " WHERE id = ?"
      log.info(s"$tableName Update Stmt: $updateStatement")

      // Get inputs required for updatePartition call.
      val rddSchema = latestChangeForEachID.schema
      val dialect = JdbcDialects.get(clientConfig.jdbcConnectionMerged.jdbcUrl)
      val batchSize = 1000 //maybe make this configurable.

      // Need to determine optimal number of partitions (set above).
      // All records in each partition are updated in a single transaction
      // if the database supports transactions.  Maybe make configurable.
      // Also consider applying commits once for each batchsize of records.
      latestChangeForEachID.foreachPartition { partition =>
        // Note : One connection per partition (better way may be to use connection pools).
        val sqlExecutorConnection: Connection = DriverManager.getConnection(clientConfig.jdbcConnectionMerged.jdbcUrl,
          clientConfig.jdbcConnectionMerged.jdbcUsername,
          clientConfig.jdbcConnectionMerged.jdbcPassword)
        updatePartition(sqlExecutorConnection, tableName, partition, rddSchema, updateStatement, batchSize, dialect)
      }
      latestChangeForEachID.unpersist()
    }

    // Filter for records to be deleted.
    // Deletes should be relatively rare since most data is retired in InsuranceSuite rather than deleted.
    val DeleteDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(1))
      .selectExpr("id")
      .coalesce(1)

    // Log number of records to be deleted.
    val delCnt = DeleteDF.count()
    log.info(s"$tableName delete cnt after filter: ${delCnt.toString}")

    // Generate and apply delete statements.
    if (delCnt > 0) {
      //val rddSchema = StructType(List(StructField("id",LongType,nullable = true)))
      val rddSchema = DeleteDF.schema
      val dialect = JdbcDialects.get(clientConfig.jdbcConnectionMerged.jdbcUrl)
      val batchSize = 1000 //maybe make this configurable.
      // Build the sql Delete statement to be used as a prepared statement for the Updates.
      val deleteStatement = "DELETE FROM " + tableName + " WHERE id = ?"
      log.info(deleteStatement)
      DeleteDF.foreachPartition { partition =>
        // Note : Once connection per partition (better way may be to use connection pools).
        val sqlExecutorConnection2: Connection = DriverManager.getConnection(clientConfig.jdbcConnectionMerged.jdbcUrl,
          clientConfig.jdbcConnectionMerged.jdbcUsername,
          clientConfig.jdbcConnectionMerged.jdbcPassword)
        updatePartition(sqlExecutorConnection2, tableName, partition, rddSchema, deleteStatement, batchSize, dialect)
      }
      DeleteDF.unpersist()
    }
  }

  /**
   *  Generate prepared sql statements and populate parameters.
   *
   * @param conn is the database connection
   * @param table is the table name
   * @param iterator contains the rows to be updated
   * @param rddSchema has the schema definition for data set from which parameters will be derived.
   * @param updateStmt is the string to be prepared as the DML statement
   * @param batchSize is the number of statements to be submitted in each batch.
   * @param dialect is the JdbcDialect based on the connection url.
   */
  private def updatePartition(conn: Connection,
                              table: String,
                              iterator: Iterator[Row],
                              rddSchema: StructType,
                              updateStmt: String,
                              batchSize: Int,
                              dialect: JdbcDialect
                             ): Unit = {

    var committed = false

    val supportsTransactions = conn.getMetaData.supportsTransactions()
    if (supportsTransactions) {
      conn.setAutoCommit(false) // Everything in the same db transaction.
    }

    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val stmt = conn.prepareStatement(updateStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0

        while (iterator.hasNext) {
          //log.info("iterator.hasNext")
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              //log.info("row.isNullAt(i) in ")
              stmt.setNull(i + 1, nullTypes(i))
              //log.info("row.isNullAt(i) out ")
            } else {
              //log.info("rsetters(i).apply(stmt, row, i) in ")
              setters(i).apply(stmt, row, i)
              //log.info("rsetters(i).apply(stmt, row, i) out ")
            }
            i = i + 1
          }
          //log.info("stmt.addBatch() in ")
          stmt.addBatch()
          //log.info("stmt.addBatch() out ")
          rowCount += 1
          //log.info(s"batch stmt added: ${rowCount.toString}")
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            //log.info(s"executeBatch Start - ${rowCount.toString} rows - $updateStmt")
            stmt.executeBatch()
            //log.info(s"executeBatch End - ${rowCount.toString} rows - $updateStmt")
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          //log.info(s"executeBatch Start - ${rowCount.toString} rows - $updateStmt")
          stmt.executeBatch()
          //log.info(s"executeBatch End - ${rowCount.toString} rows - $updateStmt")
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        log.info(s"Catch exception for $table")
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          log.info(s"Rollback for $table")
          conn.rollback()
        } else {
          log.info(s"Total rows updated in partition for $table: $totalRowCount")
        }
        conn.close()
      } else {
        log.info(s"Total rows updated in partition for $table: $totalRowCount")
        conn.close()
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
   * @param excludeInternalColumns indicates wether or not to exclude internal 'gwcbi__' columns
   *                               when comparing schemas.  When merging data we remove those columns
   *                               from the data set before saving the data so we don't want to check
   *                               for them when comparing to the schema definition in the database.
   * @return Boolean indicating if the table schema definition is the same as the parquet file schema definition
   */
  def schmasAreConsistent(dataFrame: DataFrame, jdbcSchemaName: String, tableName: String, schemaFingerprint: String, url: String,
                          user: String, pswd: String, spark: SparkSession, excludeInternalColumns: Boolean): Boolean = {

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
      val tableSchemaDef = jdbcDF.schema.toList
      val dfSchemaDef = if (excludeInternalColumns) {
        dataFrame.schema.filterNot(_.name.toLowerCase.startsWith("gwcbi___"))
      } else {
        dataFrame.schema.toList
      }
      if(tableSchemaDef==dfSchemaDef) {
        //log.info(s"File schema MATCHES Table schema")
        true
      } else {
        val jdbcType = if (excludeInternalColumns) {
          "Merged" }
        else {
          "Raw"
        }
        val logMsg = (s"""
                         |
                         | $jdbcType table definition for '$tableName' does not match parquet fingerprint '$schemaFingerprint'.  Bypassing updates for fingerprint $schemaFingerprint.
                         |
                         | $tableName $jdbcType DB Table Schema:
                         | ${"-" * (tableName.length + jdbcType.length + 18)}
                         | ${tableSchemaDef.toString.replace("StructField","").stripPrefix("List(").stripSuffix(")")}
                         |
                         | $tableName Parquet Schema for Fingerprint $schemaFingerprint:
                         | ${"-" * (tableName.length + schemaFingerprint.length + 33)}
                         | ${dfSchemaDef.toString.replace("StructField","").stripPrefix("List(").stripSuffix(")")}
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
    val tables = dbm.getTables(null, null, tableName, null)
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
