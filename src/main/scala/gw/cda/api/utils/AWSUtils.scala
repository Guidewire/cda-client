package gw.cda.api.utils

import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.AmazonS3URI

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.io.Source

object AWSUtils {

  object S3Utils {
    def getFileAsString(filePath: String): String = {
      val amazonS3URI = new AmazonS3URI(new URI(filePath))
      val objectMetadata = S3ClientSupplier.s3Client.getObject(new GetObjectRequest(amazonS3URI.getBucket, amazonS3URI.getKey))
      Source.fromInputStream(objectMetadata.getObjectContent)(StandardCharsets.UTF_8).mkString
    }

    def doesFileExist(filePath: String): Boolean = {
      val amazonS3URI = new AmazonS3URI(new URI(filePath))
      S3ClientSupplier.s3Client.doesObjectExist(amazonS3URI.getBucket, amazonS3URI.getKey)
    }

    def doesPathExists(path: String): Boolean = {
      val amazonS3URI = new AmazonS3URI(new URI(path))
      val result = S3ClientSupplier.s3Client.listObjectsV2(amazonS3URI.getBucket, amazonS3URI.getKey)
      result.getKeyCount > 0
    }
  }
}