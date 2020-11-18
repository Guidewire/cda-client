package gw.cda.api.outputwriter

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.Locale

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, functions => sqlfun}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType


private[outputwriter] class JdbcOutputWriter(override val outputPath: String, override val includeColumnNames: Boolean,
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

  override def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableName = tableDataFrameWrapperForMicroBatch.tableName

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

  /** Write RAW data to a JDBC target database.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def writeJdbcRaw(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch, connection: Connection): Unit = {

    val tableName = clientConfig.jdbcConnectionRaw.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName

    log.info(s"*** Writing '${tableDataFrameWrapperForMicroBatch.tableName}' raw data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")

    val insertDF = tableDataFrameWrapperForMicroBatch.dataFrame
    insertDF.cache()

    // Determine if we need to create the table by checking if the table already exists.
    val url = clientConfig.jdbcConnectionRaw.jdbcUrl
    val dbm = connection.getMetaData
    val dbProductName = dbm.getDatabaseProductName

    val tableNameCaseSensitive = dbProductName match {
      case "Microsoft SQL Server" | "PostgreSQL" => tableNameNoSchema
      case "Oracle"                              => tableNameNoSchema.toUpperCase
      case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val tables = dbm.getTables(connection.getCatalog(), connection.getSchema(), tableNameCaseSensitive, Array("TABLE"))
    val tableExists = tables.next()

    // Get some data we will need for later.
    val dialect = JdbcDialects.get(url)
    val insertSchema = insertDF.schema
    val batchSize = 5000 // TODO consider making this configurable.

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
    updateDataframe(connection, tableName, insertDF, insertSchema, insertStatement, batchSize, dialect, JdbcWriteType.Raw)

    log.info(s"*** Finished writing '${tableDataFrameWrapperForMicroBatch.tableName}' raw data data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
  }

  /** Merge the raw transactions into a JDBC target database applying the inserts/updates/deletes
   * according to transactions in the raw CDC data.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def writeJdbcMerged(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch, connection: Connection): Unit = {

    log.info(s"+++ Merging '${tableDataFrameWrapperForMicroBatch.tableName}' data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")

    val tableName = clientConfig.jdbcConnectionMerged.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName

    tableDataFrameWrapperForMicroBatch.dataFrame.cache()

    // Get list of CDA internal use columns to get rid of.
    val dropList = tableDataFrameWrapperForMicroBatch.dataFrame.columns.filter(colName => colName.toLowerCase.startsWith("gwcbi___"))

    // Log total rows to be merged for this fingerprint.
    val totalCount = tableDataFrameWrapperForMicroBatch.dataFrame.count()
    log.info(s"Merged - $tableName total count for all ins/upd/del: ${totalCount.toString}")

    // Filter for records to insert and drop unwanted columns.
    val insertDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(2, 0))
      .drop(dropList: _*)
      .cache()

    // Log total rows to be inserted for this fingerprint.
    val insertCount = insertDF.count()
    log.info(s"Merged - $tableName insert count after filter: ${insertCount.toString}")

    // Determine if we need to create the table by checking if the table already exists.
    val url = clientConfig.jdbcConnectionMerged.jdbcUrl
    val dbm = connection.getMetaData
    val dbProductName = dbm.getDatabaseProductName
    val tableNameCaseSensitive = dbProductName match {
      case "Microsoft SQL Server" => tableNameNoSchema
      case "PostgreSQL"           => tableNameNoSchema
      case "Oracle"               => tableNameNoSchema.toUpperCase
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val tables = dbm.getTables(connection.getCatalog(), connection.getSchema(), tableNameCaseSensitive, Array("TABLE"))
    val tableExists = tables.next

    // Get some data we will need for later.
    val dialect = JdbcDialects.get(url)
    val insertSchema = insertDF.schema
    val batchSize = 5000 // TODO consider making this configurable.

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
    updateDataframe(connection, tableName, insertDF, insertSchema, insertStatement, batchSize, dialect, JdbcWriteType.Merged)

    insertDF.unpersist()

    // Filter for records to update.
    val updateDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(4))
      .cache()

    // Log total rows marked as updates.
    val updateCount = updateDF.count()
    log.info(s"Merged - $tableName update count after filter: ${updateCount.toString}")

    // Generate and apply update statements based on the latest transaction for each id.
    if (updateCount > 0) {

      // Get the list of columns
      val colNamesArray = updateDF.columns.toBuffer
      // Remove the id and sequence from the list of columns so we can handle them separately.
      // We will be grouping by the id and the sequence will be set as the first item in the list of columns.
      colNamesArray --= Array("id", "gwcbi___seqval_hex")
      val colNamesString = "gwcbi___seqval_hex, " + colNamesArray.mkString(",")

      val latestChangeForEachID = if (clientConfig.jdbcConnectionMerged.jdbcApplyLatestUpdatesOnly) {
        // Find the latest change for each id based on the gwcbi___seqval_hex.
        // Note: For nested structs, max on struct is computed as
        // max on first struct field, if equal fall back to second fields, and so on.
        // In this case the first struct field is gwcbi___seqval_hex which will be always
        // be unique for each instance of an id in the group.
        updateDF
          .selectExpr(Seq("id", s"struct($colNamesString) as otherCols"): _*)
          .groupBy("id").agg(sqlfun.max("otherCols").as("latest"))
          .selectExpr("latest.*", "id")
          .drop(dropList: _*)
          .cache()
      } else {
        // Retain all updates.  Sort so they are applied in the correct order.
        val colVar = colNamesString + ",id"
        updateDF
          .selectExpr(colVar.split(","): _*)
          .sort(col("gwcbi___seqval_hex").asc)
          .drop(dropList: _*)
          .cache()
      }
      updateDF.unpersist()

      val latestUpdCnt = latestChangeForEachID.count()
      if (clientConfig.jdbcConnectionMerged.jdbcApplyLatestUpdatesOnly) {
        // Log row count following the reduction to only last update for each id.
        log.info(s"Merged - $tableName update count after agg to get latest for each id: ${latestUpdCnt.toString}")
      } else {
        log.info(s"Merged - $tableName all updates will be applied in sequence.")
      }

      // Build the sql Update statement to be used as a prepared statement for the Updates.
      val colListForSetClause = latestChangeForEachID.columns.filter(_ != "id")
      val colNamesForSetClause = colListForSetClause.map("\"" + _ + "\" = ?").mkString(", ")
      val updateStatement = s"""UPDATE $tableName SET $colNamesForSetClause WHERE "id" = ?"""
      //      val updateStatement = "UPDATE " + tableName + " SET " + colNamesForSetClause + " WHERE \"id\" = ?"
      log.info(s"Merged - $updateStatement")

      // Get schema info required for updatePartition call.
      val updateSchema = latestChangeForEachID.schema

      // Prepare and execute one update statement per row in our update dataframe.
      updateDataframe(connection, tableName, latestChangeForEachID, updateSchema, updateStatement, batchSize, dialect, JdbcWriteType.Merged)

      latestChangeForEachID.unpersist()
    }

    // Filter for records to be deleted.
    // Deletes should be relatively rare since most data is retired in InsuranceSuite rather than deleted.
    val deleteDF = tableDataFrameWrapperForMicroBatch.dataFrame.filter(col("gwcbi___operation").isin(1))
      .selectExpr("id")
      .cache()

    // Log number of records to be deleted.
    val deleteCount = deleteDF.count()
    log.info(s"Merged - $tableName delete count after filter: ${deleteCount.toString}")

    // Generate and apply delete statements.
    if (deleteCount > 0) {
      val deleteSchema = deleteDF.schema
      // Build the sql Delete statement to be used as a prepared statement for the Updates.
      val deleteStatement = s"""DELETE FROM $tableName WHERE "id" = ?"""
      //      val deleteStatement = "DELETE FROM " + tableName + " WHERE \"id\" = ?"
      log.info(s"Merged - $deleteStatement")

      // Prepare and execute one delete statement per row in our delete dataframe.
      updateDataframe(connection, tableName, deleteDF, deleteSchema, deleteStatement, batchSize, dialect, JdbcWriteType.Merged)

      tableDataFrameWrapperForMicroBatch.dataFrame.unpersist()
      deleteDF.unpersist()
    }
    log.info(s"+++ Finished merging '${tableDataFrameWrapperForMicroBatch.tableName}' data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
  }

  override def schemasAreConsistent(fileDataFrame: DataFrame, jdbcSchemaName: String, tableName: String, schemaFingerprint: String, url: String,
                           user: String, pswd: String, spark: SparkSession, jdbcWriteType: JdbcWriteType.Value): Boolean = {

    //Derive the product name from the url to avoid having to create or pass in a connection
    // to access the metadata object.
    val dbProductName = if (url.toLowerCase.contains("sqlserver")) {
      "Microsoft SQL Server"
    } else if (url.toLowerCase.contains("postgresql")) {
      "PostgreSQL"
    } else if (url.toLowerCase.contains("oracle")) {
      "Oracle"
    }

    if (tableExists(tableName, url, user, pswd)) {
      // build a query that returns no data from the table.  This will still get us the schema definition which is all we need.
      val sql = dbProductName match {
        case "Microsoft SQL Server" | "PostgreSQL" => s"(select * from $jdbcSchemaName.$tableName where 1=2) as $tableName"
        case "Oracle"                              => s"(select * from $jdbcSchemaName.$tableName where 1=2)"
        case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
      }
      val tableDataFrame = spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", sql)
        .option("user", user)
        .option("password", pswd)
        .load()

      val dialect = JdbcDialects.get(url)
      val tableSchemaDef = tableDataFrame.schema
      val fileSchemaDef = if (jdbcWriteType == JdbcWriteType.Merged) {
        val dropList = fileDataFrame.columns.filter(colName => colName.toLowerCase.startsWith("gwcbi___"))
        fileDataFrame.drop(dropList: _*).schema
      } else {
        fileDataFrame.schema
      }

      val databaseConnection = DriverManager.getConnection(url, user, pswd)
      databaseConnection.setAutoCommit(false)

      var newColumnAdded=false
      //ADD COLUMNS TO DATABASE TABLE THAT HAVE BEEN ADDED TO PARQUET FILE
      // Check to see if there are columns in the parquet file that are not in the database table.
      // If there are we are going to build the ALTER TABLE statement and execute the statement.
      fileSchemaDef.foreach(field => if(!scala.util.Try(tableDataFrame(field.name)).isSuccess) {
        val columnDefinition = buildDDLColumnDefinition(dialect, dbProductName.toString, tableName, field.name, field.dataType, field.nullable)
        val alterTableStatement = s"ALTER TABLE $jdbcSchemaName.$tableName ADD $columnDefinition"
        log.warn(s"Statement to be executed: $alterTableStatement")
        try {
          // Execute the table create DDL
          val stmt = databaseConnection.createStatement
          stmt.execute(alterTableStatement)
          stmt.close()
          databaseConnection.commit()
          newColumnAdded=true
          log.warn(s"ALTER TABLE - SUCCESS '$tableName' for alter table statement $alterTableStatement - $url")
        } catch {
          case e: Exception =>
            databaseConnection.rollback()
            databaseConnection.close()
            log.warn(s"ALTER TABLE - ROLLBACK '$tableName' for alter table statement $alterTableStatement - $e - $url")
            throw e
        }
      })

      databaseConnection.close()

      // Generate the table create ddl statement based on the schema definition of the database table.
      val databaseDDL = getTableCreateDDL(dialect, tableSchemaDef, tableName, jdbcWriteType, dbProductName.toString)

      // Build the create ddl statement based on the data read from the parquet file.
      val fileDDL = getTableCreateDDL(dialect, fileSchemaDef, tableName, jdbcWriteType, dbProductName.toString)

      //Compare the two table definitions and log warnings if they do not match.
      // Added twist here - we need to check to see if columns had to be added or removed from the database table.
      // If we had to ADD columns to the database table we need to rebuild the dataframe for the JDBC
      // connection and check for schema consistency using the new structures that we just performed ALTER TABLE on.
      // Since we handled all of the added or removed columns, any failure at this point will be on structure changes we
      // cannot handle via code, and the DDL differences will be logged during the second call to schemasAreConsistent.
      if (databaseDDL == fileDDL) {
        true
      } else { // instead of just logging "false", we need to check to see if there were table DDL changes executed
        if (newColumnAdded) {
          // check the schema comparison again, but now with the new table structure following ALTER statements
          val newComparison = schemasAreConsistent(fileDataFrame, jdbcSchemaName, tableName, schemaFingerprint, url, user, pswd, spark, jdbcWriteType)
          if (newComparison) { // if its fine, just return true
            true
          }
          else { // if there are still problems, return false - the second call to schemasAreConsistent will have logged any additional issues
            false
          }
        }
        else { //if there were not any ALTER statements to execute, just fail as normal and log message
          val logMsg = (s"""
                           |
                           |
                           | $jdbcWriteType table definition for '$tableName' does not match parquet fingerprint '$schemaFingerprint'.  Bypassing updates for fingerprint $schemaFingerprint.
                           |
                           | $tableName $jdbcWriteType DB Table Schema:
                           | ${"-" * (tableName.length + jdbcWriteType.toString.length + 18)}
                           | ${databaseDDL.stripPrefix(s"CREATE TABLE $tableName (").stripSuffix(")")}
                           |
                           | $tableName Parquet Schema for Fingerprint $schemaFingerprint:
                           | ${"-" * (tableName.length + schemaFingerprint.length + 33)}
                           | ${fileDDL.stripPrefix(s"CREATE TABLE $tableName (").stripSuffix(")")}
                           |""")
          log.warn(logMsg)
          log.warn(s"Database Table Schema Definition: $tableSchemaDef")
          log.warn(s"File Schema Definition: $fileSchemaDef")
          false
        }
      }
    }
    else {
      true
    }
  }

  def tableExists(tableName: String, url: String, user: String, pswd: String): Boolean = {
    val connection = DriverManager.getConnection(url, user, pswd)
    val dbm = connection.getMetaData
    val dbProductName = dbm.getDatabaseProductName
    val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
    val tableNameCaseSensitive = dbProductName match {
      case "Microsoft SQL Server" | "PostgreSQL" => tableNameNoSchema
      case "Oracle"                              => tableNameNoSchema.toUpperCase
      case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val tables = dbm.getTables(connection.getCatalog(), connection.getSchema(), tableNameCaseSensitive, Array("TABLE"))

    if (tables.next) {
      connection.close()
      true
    } else {
      connection.close()
      false
    }
  }

  /** Build and return a table create DDL statement based on the given schema definition.
   *
   * @param dialect
   * @param schema
   * @param tableName
   * @param jdbcWriteType
   * @param dbProductName
   * @return Table create DDL statement for the given table.
   */
  def getTableCreateDDL(dialect: JdbcDialect, schema: StructType, tableName: String, jdbcWriteType: JdbcWriteType.Value, dbProductName: String): String = {
    val allTableColumnsDefinitions = new StringBuilder()

    // Define specific columns we want to set as NOT NULL. Everything coming out of CDA parquet files is defined as nullable so
    // we do this to ensure there are columns available to set as PKs and/or AKs.
    var notNullCols = List("id", "gwcbi___operation", "gwcbi___seqval_hex")
    if (jdbcWriteType == JdbcWriteType.Merged) {
      // For merged data, include publicid, retired, and typecode in list of not null columns
      // so they can be included in unique index definitions.
      notNullCols = notNullCols ++ List("publicid", "retired", "typecode")
    }
    // Build the list of columns in alphabetic order.
    schema.fields.sortBy(f => f.name).foreach { field =>
      val nullable = !notNullCols.contains(field.name) && field.nullable
      val columnDefinition = buildDDLColumnDefinition(dialect, dbProductName, tableName, field.name, field.dataType, nullable)
      allTableColumnsDefinitions.append(s"$columnDefinition, ")
    }
    // Remove the trailing comma.
    val colsForCreateDDL = allTableColumnsDefinitions.stripSuffix(", ")
    // Build and return the final create table statement.
    s"CREATE TABLE $tableName ($colsForCreateDDL)"
  }

  /** Build and return a column definition to be used in CREATE and ALTER DDL statements.
   *
   * @param dialect
   * @param dbProductName
   * @param tableName
   * @param fieldName
   * @param fieldDataType
   * @param fieldNullable
   * @return Column definition - COLUMN_NAME TYPE_DECLARATION NULLABLE (i.e. '"ColumnName" VARCHAR(1333) NOT NULL').
   */
  def buildDDLColumnDefinition(dialect: JdbcDialect, dbProductName: String, tableName: String, fieldName: String, fieldDataType: DataType, fieldNullable: Boolean): String = {
    val columnDefinition = new StringBuilder()

    // Explicitly set the data type for string data to avoid nvarchar(max) and varchar2 types that are potentially too long or short.
    // nvarchar(max) columns can't be indexed.  Oracle JDBC converts the string datatype to VARCHAR2(255) which is potentially too short.
    val stringDataType = dbProductName match {
      case "Microsoft SQL Server" | "PostgreSQL" => "VARCHAR(1333)"
      case "Oracle"               => "VARCHAR2(1333)"
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    // Also for string data we need to handle very large text columns that we know of to avoid truncation sql exceptions.
    val largeStringDataType = dbProductName match {
      case "Microsoft SQL Server" => "VARCHAR(max)"
      case "PostgreSQL"           => "VARCHAR"
      case "Oracle"               => "VARCHAR2(32767)" // requires MAX_STRING_SIZE Oracle parameter to be set to EXTENDED.
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    // Also for BLOB data we need to handle differently for different platforms.
    val blobDataType = dbProductName match {
      case "Microsoft SQL Server" => "VARBINARY(max)"
      case "PostgreSQL"           => "bytea"
      case "Oracle"               => "BLOB"
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val fieldNameQuoted = dialect.quoteIdentifier(fieldName)
    val fieldDataTypeDefinition = if (fieldDataType == StringType) {
      // TODO Consider making the determination for the need for very large text columns configurable.
      // These are the OOTB columns we have found so far.
      val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
      (tableNameNoSchema, fieldName) match {
        case ("cc_outboundrecord", "\"content\"") |
             ("cc_contactorigvalue", "\"origvalue\"") |
             ("pc_diagratingworksheet", "\"diagnosticcapture\"") |
             ("cc_note", "\"body\"") => largeStringDataType
        case _                       => stringDataType
      }
    }
    else if (fieldDataType == BinaryType) blobDataType
    else if (dbProductName == "Oracle" && fieldDataType.toString.substring(0,7)=="Decimal") {
      fieldDataType match {
        case t: DecimalType => {
          if(t.scale == 0) {
            s"NUMBER(${t.precision})"
          } else {
            s"DECIMAL(${t.precision},${t.scale})"
          }
        }
      }
    }
    else getJdbcType(fieldDataType, dialect).databaseTypeDefinition
    val nullableQualifier = if (!fieldNullable) "NOT NULL" else ""
    columnDefinition.append(s"$fieldNameQuoted $fieldDataTypeDefinition $nullableQualifier")
    columnDefinition.toString()
  }

  /**
   * @param connection    database connection
   * @param url           used to determine db platform
   * @param tableName     name of the table without the schema prefix
   * @param jdbcWriteType indicates Raw or Merged data write type
   */
  private def createIndexes(connection: Connection, url: String, tableName: String, jdbcWriteType: JdbcWriteType.Value): Unit = {
    val stmt = connection.createStatement
    val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
    if (url.toLowerCase.contains("sqlserver") || url.toLowerCase.contains("postgresql") || url.toLowerCase.contains("oracle")) {

      // Create primary key.
      var ddlPrimaryKey = s"ALTER TABLE $tableName ADD CONSTRAINT ${tableNameNoSchema}_pk PRIMARY KEY "
      if (jdbcWriteType == JdbcWriteType.Merged) {
        ddlPrimaryKey = ddlPrimaryKey + "(\"id\")"
      }
      else {
        ddlPrimaryKey = ddlPrimaryKey + "(\"id\", \"gwcbi___seqval_hex\")"
      }
      log.info(s"$jdbcWriteType - $ddlPrimaryKey")
      stmt.execute(ddlPrimaryKey)

      // Create index for Merged data.  Raw data will not have any additional indexes since columns other than
      // the PK can be null (due to records for deletes).
      if (jdbcWriteType == JdbcWriteType.Merged) {
        var ddlIndex = s"CREATE INDEX ${tableNameNoSchema}_idx1 ON $tableName "
        if (tableNameNoSchema.startsWith("pctl_") || tableNameNoSchema.startsWith("cctl_") || tableNameNoSchema.startsWith("bctl_") || tableNameNoSchema.startsWith("abtl_")) {
          ddlIndex = ddlIndex + "(\"typecode\")"
        }
        else {
          ddlIndex = ddlIndex + "(\"publicid\")"
        }
        log.info(s"$jdbcWriteType - $ddlIndex")
        stmt.execute(ddlIndex)
      }

    } else {
      log.info(s"Unsupported database.  $url. Indexes were not created.")
      stmt.close()
      throw new SQLException(s"Unsupported database platform: $url")
    }

    stmt.close()
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
    var totalRowCount = 0L
    val dbProductName = conn.getMetaData.getDatabaseProductName
    try {
      val stmt = conn.prepareStatement(updateStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      //For Oracle only - map nullTypes to TINYINT for Boolean to work around Oracle JDBC driver issues
      val nullTypes = rddSchema.fields
        .map(f => if (dbProductName == "Oracle" && f.dataType == BooleanType) JdbcType("BYTE", java.sql.Types.TINYINT).jdbcNullType
        else getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0

        df.collect().foreach { row =>
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
      case IntegerType    => Some(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType       => Some(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType     => Some(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType      => Some(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType      => Some(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType       => Some(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType    => Some(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType     => Some(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType     => Some(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType  => Some(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType       => Some(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Some(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _              => None
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
    case IntegerType      =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))
    case LongType         =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))
    case DoubleType       =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))
    case FloatType        =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))
    case ShortType        =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))
    case ByteType         =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))
    case BooleanType      =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))
    case StringType       =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))
    case BinaryType       =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))
    case TimestampType    =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))
    case DateType         =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))
    case t: DecimalType   =>
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
    case _                =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

}

