package com.guidewire.cda

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

import gw.cda.api.utils.ObjectMapperSupplier
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager

import scala.io.Source

/**
 * This class will load a savepoints.json file, that allows for a configurable directory
 *
 * @param directoryPath is a directory, of where to store/find the actual savepoints json file
 */
class SavepointsProcessor(directoryPath: String) {

  private[cda] type SavepointData = scala.collection.mutable.Map[String, String]
  private val log = LogManager.getLogger
  private[cda] val savepointsDirPath = directoryPath
  private[cda] val savepointsFileName = "savepoints.json"
  private[cda] val savepointsFilePath = s"${this.savepointsDirPath}/$savepointsFileName"
  private[cda] val savepointsFileExists = checkExists()
  private[cda] val savepointsDataMap: SavepointData = readSavepointsFile()

  /** Check to see if there has been a savepoints json that already exists and return true or false.
   * Additionally, validate that the directory under which the savepoints json can be found does exist if
   * no savepoint currently exists, so that we can write to it.
   *
   * @return Boolean if there exists an already written savepoints json
   */
  private[cda] def checkExists(): Boolean = {
    if (!Files.isDirectory(Paths.get(this.savepointsDirPath))) {
      throw new IOException(s"Savepoints path ${this.savepointsDirPath} doesn't exist or isn't a directory")
    }
    // The directory exists, see if the file exists
    val doesFileExist = Files.exists(Paths.get(this.savepointsFilePath))
    doesFileExist
  }

  /** Read the contents of a savepoints json file into a map, which contains the last read timestamp for each table.
   *
   * @return Map of tablename to last read timestamp
   */
  private[cda] def readSavepointsFile(): SavepointData = {
    if (this.savepointsFileExists) {
      log.info(s"Savepoints file '${this.savepointsFilePath}' exists")
      val savepointsSource = Source.fromFile(this.savepointsFilePath)
      try {
        val savepointsJson = savepointsSource.getLines.mkString
        ObjectMapperSupplier.jsonMapper.readValue(savepointsJson, classOf[SavepointData])
      } finally {
        savepointsSource.close
      }
    } else {
      log.info(s"Savepoints file '${this.savepointsFilePath}' does not exist")
      scala.collection.mutable.Map.empty
    }
  }

  /** Get the last read timestamp for a table. Return an Option[String] with the last read
   * timestamp. This is used to list timestamp subfolders in s3 starting from the timestamp.
   *
   * @param tableName String name of table
   * @return Option[String] of last read timestamp of table
   */
  private[cda] def getSavepoint(tableName: String): Option[String] = {
    this.savepointsDataMap.get(tableName)
  }

  /** Write the timestamp for the table into the savepoints file.
   * Example entry in savepoints json:
   * {
   * "note": "1562112543749"
   * ...
   * }
   *
   * WARNING - The method is synchronized to ensure only 1 write at a time; the method is fast so it should be okay
   *
   * @param tableName             Table to update for SavePoint
   * @param newSavePointTimestamp the timestamp to use for the update
   */
  private[cda] def writeSavepoints(tableName: String, newSavePointTimestamp: String): Unit = synchronized {
    val newSavePointTuple = (tableName, newSavePointTimestamp)
    log.info(s"Updating savepoints file '${this.savepointsFilePath}', with=$newSavePointTuple")
    savepointsDataMap.put(tableName, newSavePointTimestamp) // upsert the instance variable with the new save point
    val newSavepointsJson = ObjectMapperSupplier.jsonMapper.writerWithDefaultPrettyPrinter.writeValueAsString(savepointsDataMap)
    val newSavepointsFile = new File(savepointsFilePath)
    FileUtils.writeStringToFile(newSavepointsFile, newSavepointsJson, null: String)
    log.info(s"Updated savepoints file")
  }

}