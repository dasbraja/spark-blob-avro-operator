package starbucks.bits.siot

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import starbucks.bits.siot.Conf.{appName, blobAccount, blobKey, driverMemory, executorMemory, outblobAccount, outblobAccount2, outblobKey, outblobKey2, sparkMaster}

object Session extends LazyLogging {

  def apply(): SparkSession = {
    val session = SparkSession
      .builder()
      .appName(appName)
      .master(sparkMaster)
      .config("spark.executor.memory", executorMemory)
      .config("spark.driver.memory", driverMemory)
      .config("spark.storage.memoryFraction", "1")
      .config("spark.streaming.backpressure.enabled", "1")
      .config("spark.streaming.receiver.maxRate", "100")
      .config("spark.streaming.backpressure.initialRate", "100")
      .getOrCreate()

    session.sparkContext.hadoopConfiguration
      .set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    session.sparkContext.hadoopConfiguration
      .set(s"fs.azure.account.key.$blobAccount.blob.core.windows.net", blobKey)
    session.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    session.sparkContext.hadoopConfiguration
      .set(s"fs.azure.account.key.$outblobAccount.blob.core.windows.net", outblobKey)
    session.sparkContext.hadoopConfiguration
      .set(s"fs.azure.account.key.$outblobAccount2.blob.core.windows.net", outblobKey2)

    logger.info(s"using checkpoint azure blob account $blobAccount")

    session
  }


}
