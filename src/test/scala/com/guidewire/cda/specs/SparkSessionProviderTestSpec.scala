package com.guidewire.cda.specs

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.util.Failure
import scala.util.Try

abstract class SparkSessionProviderTestSpec extends UnitTestSpec {
  private val logger = LogManager.getLogger
  private val sparkCatalogFolderPrefix = "spark-warehouse-for-test-"
  private var hadoopFileSystem: FileSystem = _
  val sparkCatalogFolderName: String = s"$sparkCatalogFolderPrefix${System.currentTimeMillis()}"
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    val sparkSessionBuilder = createSparkSessionBuilder()
    sparkSession = sparkSessionBuilder
      .getOrCreate

    hadoopFileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    deleteSparkCatalogTestFolders()
  }

  override def afterAll(): Unit = {
    SparkSession.clearActiveSession()
    sparkSession.close()

    deleteSparkCatalogTestFolders()
  }

  def extendedAfter(): Unit = {
    logger.info(
      """ ScalaTest only allows for a suite to call `after` once. To avoid having to implement the
        | `deleteSparkCatalog` call in every implementation of SparkSessionProviderTestSpec, we call `after` here.
        | If a class needs to do additional tasks in the `after` stage, override the
        | `SparkSessionProviderTestSpec.extendedAfter` method.""".stripMargin)
  }

  after {
    deleteSparkCatalog()
    extendedAfter()
    sparkSession.catalog.setCurrentDatabase("default")
  }

  protected def createSparkSessionBuilder(): SparkSession.Builder = {
    SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.debug.maxToStringFields", "100000")
      .config("spark.rdd.compress", "true")
      .config("spark.task.maxFailures", "25")
      .config("spark.streaming.unpersist", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", sparkCatalogFolderName)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.default.parallelism", "2")
      .appName("PowerlineTest")
      .master("local[2]")
  }

  private def deleteSparkCatalog(): Unit = {
    val tableNames = sparkSession
      .catalog
      .listTables()
      .collect()
      .map(table => table.database match {
        case null => table.name
        case _    => s"${table.database}.${table.name}"
      })

    tableNames.foreach(databaseName =>
      sparkSession.sql(s"DROP TABLE IF EXISTS $databaseName")
    )
  }

  private def deleteSparkCatalogTestFolders(): Unit = {
    val acceptOnlySparkCatalogFolderPaths = new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith(sparkCatalogFolderPrefix)
    }

    hadoopFileSystem.listStatus(hadoopFileSystem.getWorkingDirectory, acceptOnlySparkCatalogFolderPaths)
      .foreach(fileStatus => {
        Try(hadoopFileSystem.delete(fileStatus.getPath, true)) match {
          case Failure(e) => logger.error(s"Couldn't delete the following file: ${fileStatus.getPath.getName}: $e")
          case _          =>
        }
      })
  }
}
