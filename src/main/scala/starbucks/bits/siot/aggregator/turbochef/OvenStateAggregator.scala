package starbucks.bits.siot.aggregator.turbochef

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object OvenStateAggregator extends LazyLogging {

  def getDeviceOvenStateAgg(session: SparkSession, df: DataFrame) = {
    logger.info("  TurboChef MachineEvent Oven State Aggregator  ---")
    logger.info("Started processing: ---- oven state count by device_id, cook_chamber ---------")
    df.createOrReplaceTempView("ovenstate")
    session.sqlContext.sql(
      "select device_id, cook_chamber, state, count(*) cnt " +
        "from ovenstate " +
        "group by device_id, cook_chamber, state"
    )
  }

  def getHourlyOvenStateAgg(session: SparkSession, df: DataFrame) = {
    logger.info("  TurboChef MachineEvent Oven State Aggregator  ---")
    logger.info("Started processing: ---- oven state count by cook_chamber, eventdate, eventhour ---------")

    df.createOrReplaceTempView("ovenstate")
    session.sqlContext.sql(
      "select cook_chamber, state, substring(eventtime, 0,10) eventdate,  substring(eventtime, 12,2) hour, " +
        "count(*) cnt " +
        "from ovenstate " +
        "group by cook_chamber, state, eventdate , hour"
    )
  }

  def getDailyOvenStateAgg(session: SparkSession, df: DataFrame) = {
    logger.info("  TurboChef MachineEvent Oven State Aggregator  ---")
    logger.info("Started processing: ---- oven state count by cook_chamber, eventdate ---------")

    df.createOrReplaceTempView("ovenstate")
    session.sqlContext.sql(
      "select cook_chamber, state, substring(eventtime, 0,10) eventdate, " +
        "count(*) cnt " +
        "from ovenstate " +
        "group by cook_chamber, state, eventdate"
    )
  }


}
