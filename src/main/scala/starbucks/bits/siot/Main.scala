package starbucks.bits.siot

import com.typesafe.scalalogging.LazyLogging

object Main extends LazyLogging{


  def main(args: Array[String]): Unit = {
    //AvroOperator.readAvroWriteJsonBatch(batchavrojsonload)
    //Processor.processHeartbeatAggregate()
    Processor.processDisConnectedSets()

  }

}
