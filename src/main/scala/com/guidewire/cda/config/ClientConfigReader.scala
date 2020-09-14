package com.guidewire.cda.config

import com.fasterxml.jackson.databind.exc.InvalidFormatException
import gw.cda.api.utils.ObjectMapperSupplier

import scala.io.Source

/*
  -Jackson will set Boolean types to false if left blank in yaml file
  -The 'var' keyword is used to make a section/parameter optional
 */

private[cda] case class SourceLocation(bucketName: String,
                                       manifestKey: String)

private[cda] case class OutputLocation(path: String)

private[cda] case class SavepointsLocation(path: String)

private[cda] case class OutputSettings(tablesToInclude: String,
                                       saveIntoJdbcRaw: Boolean,
                                       saveIntoJdbcMerged: Boolean,
                                       saveIntoFile: Boolean,
                                       fileFormat: String,
                                       includeColumnNames: Boolean,
                                       saveAsSingleFile: Boolean,
                                       saveIntoTimestampDirectory: Boolean)

private[cda] case class PerformanceTuning(var numberOfJobsInParallelMaxCount: Int,
                                          var numberOfThreadsPerJob: Int)

private[cda] case class SparkTuning(maxResultSize: String,
                                    driverMemory: String,
                                    executorMemory: String)

/** */
private[cda] case class JdbcConnectionRaw(jdbcUsername: String,
                                          jdbcPassword: String,
                                          jdbcUrl: String,
                                          jdbcSchema: String,
                                          jdbcSaveMode: String)

private[cda] case class JdbcConnectionMerged(jdbcUsername: String,
                                             jdbcPassword: String,
                                             jdbcUrl: String,
                                             jdbcSchema: String,
                                             jdbcApplyLatestUpdatesOnly: Boolean)

case class ClientConfig(sourceLocation: SourceLocation,
                        outputLocation: OutputLocation,
                        savepointsLocation: SavepointsLocation,
                        outputSettings: OutputSettings,
                        var performanceTuning: PerformanceTuning,
                        sparkTuning: SparkTuning,
                        jdbcConnectionRaw: JdbcConnectionRaw,
                        jdbcConnectionMerged: JdbcConnectionMerged)

object ClientConfigReader {

  /** Function to fetch config.yaml file and parse it into a map,
   * then validate the file is logically correct, and all the expected parameters exist.
   *
   * @param configFilePath String path to the config.yaml file
   * @return ClientConfig instance with config fields
   */
  def processConfigFile(configFilePath: String): ClientConfig = {
    val clientConfig = parseConfig(configFilePath)
    validate(clientConfig)
    clientConfig
  }

  /** Fetch config.yaml file and parse it into a scala map
   *
   * @param configPath String path to config.yaml file
   * @return Config case class with config fields
   */
  private[cda] def parseConfig(configPath: String): ClientConfig = {
    val configSource = Source.fromFile(configPath)
    try {
      val configYaml = configSource.getLines.mkString("\n")
      ObjectMapperSupplier.yamlMapper.readValue(configYaml, classOf[ClientConfig])
    } catch {
      case e: InvalidFormatException => throw InvalidConfigParameterException(s"There was an error while parsing the config file ($configPath), look at the CausedBy exception for details.", e)
    } finally {
      configSource.close
    }
  }

  /** Ensures that the config file that was read in was formatted properly, that no parameter sections are missing,
   * and that none of the location parameters were left blank. Throws exceptions if the config does not meet these requirements.
   *
   * @param clientConfig instance to validate
   */
  private[cda] def validate(clientConfig: ClientConfig): Unit = {
    validateSourceLocation(clientConfig)
    validateOutputLocation(clientConfig)
    validateSavepointsLocation(clientConfig)
    validateOutputSettings(clientConfig)
    validatePerformanceTuning(clientConfig)
    validateSparkTuning(clientConfig)
    validateJdbcConnectionRaw(clientConfig)
    validateJdbcConnectionMerged(clientConfig)
  }

  /** validateSourceLocation
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validateSourceLocation(clientConfig: ClientConfig): Unit = {
    try {
      require(clientConfig.sourceLocation != null, "sourceLocation section is missing in the config file")
    } catch {
      case e: IllegalArgumentException => throw MissingConfigParameterException("Config section is missing from the config file", e)
    }

    try {
      require(clientConfig.sourceLocation.bucketName != null, "sourceLocation.bucketName is blank")
      require(clientConfig.sourceLocation.manifestKey != null, "sourceLocation.manifestKey is blank")
    } catch {
      case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
    }
  }

  /** validateOutputLocation
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validateOutputLocation(clientConfig: ClientConfig): Unit = {
    try {
      require(clientConfig.outputLocation != null, "outputLocation section is missing in the config file")
    } catch {
      case e: IllegalArgumentException => throw MissingConfigParameterException("Config section is missing from the config file", e)
    }

    try {
      require(clientConfig.outputLocation.path != null, "outputLocation.path is blank")
    } catch {
      case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
    }

    try {
      require(clientConfig.outputLocation.path.endsWith("/") != true, "outputLocation.path has a trailing slash, remove it")
    } catch {
      case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter has an invalid value", e)
    }
  }

  /** validateOutputSettings
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validateOutputSettings(clientConfig: ClientConfig): Unit = {
    try {
      require(clientConfig.outputSettings != null, "outputSettings section is missing in the config file")
    } catch {
      case e: IllegalArgumentException => throw MissingConfigParameterException("Config section is missing from the config file", e)
    }

    //All boolean parameters will get a default value of false if they are not in the config.yaml file

    //If saving to file then file format must be either csv or parquet.
    if (clientConfig.outputSettings.saveIntoFile) {
      val validOptions = List("csv", "parquet")
      try {
        require(validOptions.contains(clientConfig.outputSettings.fileFormat.toLowerCase()),
          "outputSettings.fileFormat is is not valid.  Valid options are 'csv' or 'parquet'.")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is invalid in the config file", e)
      }
    }
  }

  /** validatePerformanceTuning - this section is totally optional, since most users wont know what to set the values to
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validatePerformanceTuning(clientConfig: ClientConfig): Unit = {
    // Reasonable defaults for when these are excluded from the config file
    val processorCount = Runtime.getRuntime.availableProcessors
    val defaultThreadsPerJob = 10

    //If the whole section was excluded from the config file
    if (clientConfig.performanceTuning == null) {
      clientConfig.performanceTuning = PerformanceTuning(processorCount, defaultThreadsPerJob)
    }

    //In case only this parameter was excluded from the config file
    if (clientConfig.performanceTuning.numberOfJobsInParallelMaxCount == 0) {
      clientConfig.performanceTuning.numberOfJobsInParallelMaxCount = processorCount
    }

    //In case only this parameter was excluded from the config file
    if (clientConfig.performanceTuning.numberOfThreadsPerJob == 0) {
      clientConfig.performanceTuning.numberOfThreadsPerJob = defaultThreadsPerJob
    }
  }

  /**
   * Assert that a condition holds for a parameter value, throwing an exception if it doesn't.
   *
   * @param condition    The condition to check.
   * @param errorMessage The error message provided if the condition doesn't hold.
   * @throws InvalidConfigParameterException if the condition doesn't hold.
   */
  private def validateParameterValue(condition: Boolean, errorMessage: String): Unit = {
    try {
      require(condition, errorMessage)
    } catch {
      case ex: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter has an invalid value", ex)
    }
  }

  private def validateSparkTuning(clientConfig: ClientConfig): Unit = {
    Option(clientConfig.sparkTuning).foreach(sparkTuning => {
      val validMemoryArgumentRegex = "[1-9][0-9]*[mMgG]"

      def isValidMemoryArgument(x: String): Boolean = {
        x.matches(validMemoryArgumentRegex)
      }

      Option(sparkTuning.driverMemory)
        .foreach(driverMemory => validateParameterValue(isValidMemoryArgument(driverMemory),
          s"sparkTuning.driverMemory value '$driverMemory' does not match the required format $validMemoryArgumentRegex"))
      Option(sparkTuning.executorMemory)
        .foreach(executorMemory => validateParameterValue(isValidMemoryArgument(executorMemory),
          s"sparkTuning.executorMemory value '$executorMemory' does not match the required format $validMemoryArgumentRegex"))
      Option(sparkTuning.maxResultSize)
        .foreach(maxResultSize => validateParameterValue(isValidMemoryArgument(maxResultSize) || maxResultSize == "0",
          s"sparkTuning.maxResultSize value '$maxResultSize' must be 0 or match the this format: $validMemoryArgumentRegex"))
    })
  }

  /** validateSavepointsLocation
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validateSavepointsLocation(clientConfig: ClientConfig): Unit = {
    try {
      require(clientConfig.savepointsLocation != null, "savepointsLocation section is missing in the config file")
    } catch {
      case e: IllegalArgumentException => throw MissingConfigParameterException("Config section is missing from the config file", e)
    }

    try {
      require(clientConfig.savepointsLocation.path != null, "savepointsLocation.path is blank")
    } catch {
      case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
    }

    try {
      require(clientConfig.savepointsLocation.path.endsWith("/") != true, "savepointsLocation.path has a trailing slash, remove it")
    } catch {
      case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter has an invalid value", e)
    }
  }

  /** validateJdbcConnectionRaw
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validateJdbcConnectionRaw(clientConfig: ClientConfig): Unit = {
    // If saving to JDBC then validate the jdbcConnection section.
    if (clientConfig.outputSettings.saveIntoJdbcRaw || clientConfig.outputSettings.saveIntoJdbcMerged) {
      try {
        require(clientConfig.jdbcConnectionRaw != null, "jdbcConnectionRaw section is missing in the config file")
      } catch {
        case e: IllegalArgumentException => throw MissingConfigParameterException("Config section is missing from the config file", e)
      }

      try {
        require(clientConfig.jdbcConnectionRaw.jdbcUrl != null, "jdbcConnectionRaw.jdbcUrl is blank")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
      }

      try {
        require(clientConfig.jdbcConnectionRaw.jdbcSchema != null, "jdbcConnectionRaw.jdbcSchema is blank")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
      }

      try {
        require(clientConfig.jdbcConnectionRaw.jdbcSaveMode != null, "jdbcConnectionRaw.jdbcSaveMode is blank")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
      }
      val validOptions = List("overwrite", "append")
      try {
        require(validOptions.contains(clientConfig.jdbcConnectionRaw.jdbcSaveMode.toLowerCase()),
          "jdbcConnection.jdbcSaveMode is is not valid.  Valid options are 'overwrite' or 'append'.")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is invalid in the config file", e)
      }

    }
  }

  /** validateJdbcConnectionMerged
   *
   * @param clientConfig instance to validate, for performance settings
   */
  private def validateJdbcConnectionMerged(clientConfig: ClientConfig): Unit = {
    // If saving to JDBC then validate the jdbcConnection section.
    if (clientConfig.outputSettings.saveIntoJdbcMerged || clientConfig.outputSettings.saveIntoJdbcMerged) {
      try {
        require(clientConfig.jdbcConnectionMerged != null, "validateJdbcConnectionMerged section is missing in the config file")
      } catch {
        case e: IllegalArgumentException => throw MissingConfigParameterException("Config section is missing from the config file", e)
      }

      try {
        require(clientConfig.jdbcConnectionMerged.jdbcUrl != null, "jdbcConnectionMerged.jdbcUrl is blank")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
      }

      try {
        require(clientConfig.jdbcConnectionMerged.jdbcSchema != null, "jdbcConnectionMerged.jdbcSchema is blank")
      } catch {
        case e: IllegalArgumentException => throw InvalidConfigParameterException("Config parameter is missing, or is left blank in the config file", e)
      }
    }
  }

}
