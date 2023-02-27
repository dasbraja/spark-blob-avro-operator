package starbucks.bits.siot.transformer

import org.apache.spark.rdd.RDD

object RddTransformer {

  def filterRddString(inputRDD: RDD[String],filter1: String, filter2: String, filter3:String) = {
    inputRDD.filter(x => x.contains(filter1) && x.contains(filter2) && x.contains(filter3))
  }

}
