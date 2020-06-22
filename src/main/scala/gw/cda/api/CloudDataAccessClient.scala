package gw.cda.api

import com.guidewire.cda.TableReader
import com.guidewire.cda.config.ClientConfig
import com.guidewire.cda.config.ClientConfigReader
import com.guidewire.cda.cli.CLIArgumentsProcessor
import org.apache.logging.log4j.LogManager

object CloudDataAccessClient {

  private val log = LogManager.getLogger

  def main(args: Array[String]): Unit = {
    // Get the command line args
    val cliArgumentsProcessor = new CLIArgumentsProcessor(args)
    val configFilePath = cliArgumentsProcessor.configFilePath
    val singleTableValue = cliArgumentsProcessor.singleTableValue

    // Log the name of the config file, so if there is a problem processing it, you will know the name of the file
    log.info(s"Loading config file '$configFilePath'")
    val clientConfig: ClientConfig = ClientConfigReader.processConfigFile(configFilePath)
    log.info(s"The config file has been loaded - $clientConfig")

    // Moved processConfig() outside of TableReader, parsing it is an unnecessary responsibility of the TableReader
    val tableReader = new TableReader(clientConfig)
    tableReader.run(singleTableValue)
  }

}
