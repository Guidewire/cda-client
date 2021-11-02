package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.utils.ObjectMapperSupplier
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.Row

import java.io.StringWriter
import java.net.URI
import scala.util.parsing.json.JSONObject

trait FileBasedOutputWriter extends OutputWriter {
  private val stringifyRowUDF: UserDefinedFunction = udf[String, Row](parseColumn)
  val schemaFileName = "schema.yaml"
  //Instance vars that are passed in during constructor of the concrete classes
  val includeColumnNames: Boolean
  val outputPath: String
  val saveAsSingleFile: Boolean
  val saveIntoTimestampDirectory: Boolean
  val clientConfig: ClientConfig

  /**
   * Validate the outputPath, making sure that it exists/is a valid directory.
   * If there is a problem, throw an exception.
   */
  override def validate(): Unit

  /** Write a table and its schema to either local filesystem or to S3
   * and also to JDBC as indicated by clientConfig settings.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  override def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableName = tableDataFrameWrapperForMicroBatch.tableName

    if (clientConfig.outputSettings.exportTarget == "file") {
      clientConfig.outputSettings.fileFormat.toLowerCase match {
        case "csv"     =>
          log.info(s"Writing '$tableName' DataFrame as CSV to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
          this.writeCSV(tableDataFrameWrapperForMicroBatch)
          this.writeSchema(tableDataFrameWrapperForMicroBatch)
          log.info(s"Wrote '$tableName' DataFrame as CSV complete, with columns ${tableDataFrameWrapperForMicroBatch.dataFrame.columns.toList}")
        case "parquet" =>
          log.info(s"Writing '$tableName' DataFrame as PARQUET to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
          this.writeParquet(tableDataFrameWrapperForMicroBatch)
          log.info(s"Wrote '$tableName' DataFrame as PARQUET complete")
        case other     => throw new Exception(s"Unknown output file format $other")
      }
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
   * @param pathPrefix                         A DataFrame corresponding to a table
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

  /** Write a table to a Parquet.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  private def writeParquet(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
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

  /** Write a table to a CSV file to either local filesystem or to S3.
   * Thanks to Spark, this is the same operation. Calls makeCSVPath to construct
   * the correct path location. Existing csv files in the same path are deleted
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  private[outputwriter] def writeCSV(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
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
}

object FileBasedOutputWriter {
  def apply(outputWriterConfig: OutputWriterConfig): FileBasedOutputWriter = {
    new URI(outputWriterConfig.outputPath).getScheme match {
      case "s3" =>
        new S3OutputWriter(outputWriterConfig.outputPath, outputWriterConfig.includeColumnNames, outputWriterConfig.saveAsSingleFile, outputWriterConfig.saveIntoTimestampDirectory, outputWriterConfig.clientConfig)
      case _    =>
        new LocalFilesystemOutputWriter(outputWriterConfig.outputPath, outputWriterConfig.includeColumnNames, outputWriterConfig.saveAsSingleFile, outputWriterConfig.saveIntoTimestampDirectory, outputWriterConfig.clientConfig)
    }
  }
}