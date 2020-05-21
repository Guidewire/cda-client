package gw.cda.api.outputwriter

import java.io.StringWriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import gw.cda.api.utils.ObjectMapperSupplier
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StructType

import scala.util.parsing.json.JSONObject


case class OutputWriterConfig(outputPath: String, includeColumnNames: Boolean, saveAsSingleFileCSV: Boolean, saveIntoTimestampDirectory: Boolean)

trait OutputWriter {

  private val log = LogManager.getLogger
  val schemaFileName = "schema.yaml"

  //Instance vars that are passed in during constructor of the concrete classes
  val outputPath: String
  val includeColumnNames: Boolean
  val saveAsSingleFileCSV: Boolean
  val saveIntoTimestampDirectory: Boolean

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

  /** Write a table and its schema to either local filesystem or to S3.
    *
    * @param tableDataFrameWrapperForMicroBatch has the data to be written
    */
  def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableName = tableDataFrameWrapperForMicroBatch.tableName
    log.info(s"Writing '$tableName' DataFrame as CSV to ${this.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch)}")
    this.writeCSV(tableDataFrameWrapperForMicroBatch)
    this.writeSchema(tableDataFrameWrapperForMicroBatch)
    log.info(s"Wrote '$tableName' DataFrame as CSV complete, with columns ${tableDataFrameWrapperForMicroBatch.dataFrame.columns.toList}")
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

    if (saveAsSingleFileCSV) {
      // This is much slower, coalesce(1) has to reshuffle all the partitions to 1
      tableDF.coalesce(1).write.option("header", includeColumnNames).mode(SaveMode.Overwrite).csv(pathToFolderWithCSV)
    } else {
      // This should be faster in theory, it writes 1 CSV per partition
      tableDF.write.option("header", includeColumnNames).mode(SaveMode.Overwrite).csv(pathToFolderWithCSV)
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
      new S3OutputWriter(outputWriterConfig.outputPath, outputWriterConfig.includeColumnNames, outputWriterConfig.saveAsSingleFileCSV, outputWriterConfig.saveIntoTimestampDirectory)
    else
      new LocalFilesystemOutputWriter(outputWriterConfig.outputPath, outputWriterConfig.includeColumnNames, outputWriterConfig.saveAsSingleFileCSV, outputWriterConfig.saveIntoTimestampDirectory)
  }

}
