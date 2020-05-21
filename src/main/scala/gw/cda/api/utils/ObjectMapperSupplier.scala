package gw.cda.api.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/** Supplier object for jackson JSON mapper and YAML mapper object instances.
 * Initializes them here so that that only one instance of each is needed throughout
 * the code
 */
object ObjectMapperSupplier {

  val jsonMapper = new ObjectMapper() with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)
  val yamlMapper = new ObjectMapper(new YAMLFactory)
  yamlMapper.registerModule(DefaultScalaModule)

}
