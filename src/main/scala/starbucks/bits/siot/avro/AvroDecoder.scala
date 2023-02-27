package starbucks.bits.siot.avro

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import starbucks.bits.siot.Conf.{blobFilePath, ehRecordSchema}
import starbucks.bits.siot.Session

object AvroDecoder {

  def parseBody(attrToCheck: String, blobFilePath: String): (SparkSession, RDD[String]) =  {
    val session: SparkSession = Session()
    val df = session
      .read
      .format("avro")
      .option("avroSchema", ehRecordSchema)
      .load(blobFilePath)

    val bodyMessage: RDD[String] = df.rdd.flatMap(row => {
      val spMap: collection.Map[String, String] =
        row.getMap(row.fieldIndex("SystemProperties"))
      spMap.get(attrToCheck) match {
        case Some(value) =>
          val bodyIdx = row.fieldIndex("Body")
          val json = row.getString(bodyIdx)
          List(json)
        case _ => List()
      }
    })
    (session, bodyMessage)
  }

}
