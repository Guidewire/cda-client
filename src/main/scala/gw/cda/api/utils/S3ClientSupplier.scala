package gw.cda.api.utils

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder

/** Supplier object for AmazonS3 client object.
 * Initializes it here so that that only one instance is needed throughout
 * the code
 */
object S3ClientSupplier {

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withForceGlobalBucketAccessEnabled(true).build()

}
