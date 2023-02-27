package starbucks.bits.siot.avro

import com.typesafe.scalalogging.LazyLogging
import starbucks.bits.siot.Conf.{batchDates, batchavrojsonload, filter1RDD, filter2RDD, filter3RDD, outblobFilePath, sysAttrCheck}
import starbucks.bits.siot.Main.logger
import starbucks.bits.siot.avrotojsonbatch
import starbucks.bits.siot.transformer.RddTransformer

object AvroOperator extends LazyLogging {

  def readAvroWriteJson(outblobFilePath: String, blobFilePath: String, partitionCount: Int) = {
    val (session, bodyMessage) = AvroDecoder.parseBody(sysAttrCheck, blobFilePath)
    val filterrdd = RddTransformer.filterRddString(
      bodyMessage,
      filter1RDD,
      filter2RDD,
      filter3RDD
    )
    filterrdd.coalesce(partitionCount).saveAsTextFile(outblobFilePath)
    session.sparkContext.stop()
  }

  def readAvroGetRDD(blobFilePath: String) = {
    val (session, bodyMessage) = AvroDecoder.parseBody(sysAttrCheck, blobFilePath)
    RddTransformer.filterRddString(
      bodyMessage,
      filter1RDD,
      filter2RDD,
      filter3RDD
    )
  }

  def readAvroWriteJsonBatch(batch: avrotojsonbatch)  = {

    val hourArr = batch.hours.split(";")
    for(hour<- hourArr) {
      val dateHour = batch.date + hour
      val inputPath = batch.inpathprefix + dateHour + batch.inpathsuffix
      val outputPath = batch.outpathprefix + dateHour + batch.outpathsuffix
      AvroOperator.readAvroWriteJson(outputPath, inputPath,4)

      logger.info(s"---- Load completed for " + dateHour + "---------------" )
    }

  }

  def readAvroWriteJsonMultipleDates(batch: avrotojsonbatch)  = {

    val hourArr = batch.hours.split(";")
    val datesArr = batchDates.split(";")
    for(date <- datesArr) {
      for(hour<- hourArr) {
        val dateHour = date + hour
        val inputPath = batch.inpathprefix + dateHour + batch.inpathsuffix
        val outputPath = batch.outpathprefix + dateHour + batch.outpathsuffix
        AvroOperator.readAvroWriteJson(outputPath, inputPath,4)

        logger.info(s"---- Load completed for " + dateHour + "---------------" )
      }
    }


  }

}
