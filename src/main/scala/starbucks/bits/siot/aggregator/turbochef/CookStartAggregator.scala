package starbucks.bits.siot.aggregator.turbochef

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object CookStartAggregator extends LazyLogging{

  def getDailyCookDurationStats(session: SparkSession, dfCookStart: DataFrame): DataFrame=  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Cook-Duration Aggregator  ---")
    logger.info("Started processing: ---- group  by cook_chamber, cook_type, eventdate ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, count(*) cnt, " +
        "avg(cook_durn) avg_cook_durn, stddev(cook_durn) stdev_cook_durn, " +
        "avg(cook_durn) + (3 * stddev(cook_durn)) UCL,  " +
        "avg(cook_durn) - (3 * stddev(cook_durn)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate"
    )
  }

  def getHourlyCookDurationStats(session: SparkSession, dfCookStart: DataFrame): DataFrame=  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Cook-Duration Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, eventhour ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, substring(eventtime, 12,2) hour, " +
        "count(*) cnt, " +
        "avg(cook_durn) avg_cook_durn, stddev(cook_durn) stdev_cook_durn, " +
        "avg(cook_durn) + (3 * stddev(cook_durn)) UCL,  " +
        "avg(cook_durn) - (3 * stddev(cook_durn)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate, hour"
    )
  }

  def getDailyRecipeCookDurationStats(session: SparkSession, dfCookStart: DataFrame): DataFrame =  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Cook-Duration Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, recipe_group ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
      session.sqlContext.sql(
        "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, recipe_group, count(*) cnt, " +
          "avg(cook_durn) avg_cook_durn, stddev(cook_durn) stdev_cook_durn, " +
          "avg(cook_durn) + (3 * stddev(cook_durn)) UCL,  " +
          "avg(cook_durn) - (3 * stddev(cook_durn)) LCL  " +
          "from cookstart " +
          "group by cook_chamber, cook_type,eventdate, recipe_group"
      )
  }

  def getDailyRecipeDetailCookDurationStats(session: SparkSession, dfCookStart: DataFrame): DataFrame =  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Cook-Duration Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, recipe_group, recipe_subgroup ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, recipe_group, recipe_subgroup, count(*) cnt, " +
        "avg(cook_durn) avg_cook_durn, stddev(cook_durn) stdev_cook_durn, " +
        "avg(cook_durn) + (3 * stddev(cook_durn)) UCL,  " +
        "avg(cook_durn) - (3 * stddev(cook_durn)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate, recipe_group, recipe_subgroup"
    )
  }

  def getDailyRecipeQtyDetailCookDurationStats(session: SparkSession, dfCookStart: DataFrame): DataFrame =  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Cook-Duration Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, recipe_group, recipe_subgroup, cook_qty ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, recipe_group, recipe_subgroup, cook_qty, count(*) cnt, " +
        "avg(cook_durn) avg_cook_durn, stddev(cook_durn) stdev_cook_durn, " +
        "avg(cook_durn) + (3 * stddev(cook_durn)) UCL,  " +
        "avg(cook_durn) - (3 * stddev(cook_durn)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, recipe_group, recipe_subgroup, cook_qty"
    )
  }

  def getDeviceDailyCookDurationStats(session: SparkSession, dfCookStart: DataFrame): DataFrame=  {
    logger.info("  TurboChef MachineEvent (Cook-Start) Cook-Duration Aggregator  ---")
    logger.info("Started processing: ---- group by device_id, cook_chamber, cook_type, eventdate ---------")

    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  device_id, cook_chamber, cook_type, count(*) cnt, " +
        "avg(cook_durn) avg_cook_durn, stddev(cook_durn) stdev_cook_durn, " +
        "avg(cook_durn) + (3 * stddev(cook_durn)) UCL,  " +
        "avg(cook_durn) - (3 * stddev(cook_durn)) LCL  " +
        "from cookstart " +
        "group by device_id, cook_chamber, cook_type"
    )
  }

  def getDailyCookOvenHeaterStats(session: SparkSession, dfCookStart: DataFrame): DataFrame=  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Oven-Heater-Temperature Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate ---------")

    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, count(*) cnt, " +
        "avg(oven_heater_temp) avg_oven_heater_temp, stddev(oven_heater_temp) stdev_oven_heater_temp, " +
        "avg(oven_heater_temp) + (3 * stddev(oven_heater_temp)) UCL,  " +
        "avg(oven_heater_temp) - (3 * stddev(oven_heater_temp)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate"
    )
  }

  def getHourlyCookOvenHeaterStats(session: SparkSession, dfCookStart: DataFrame): DataFrame=  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Oven-Heater-Temperature Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, eventhour ---------")

    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, substring(eventtime, 12,2) hour, " +
        "count(*) cnt, " +
        "avg(oven_heater_temp) avg_oven_heater_temp, stddev(oven_heater_temp) stdev_oven_heater_temp, " +
        "avg(oven_heater_temp) + (3 * stddev(oven_heater_temp)) UCL,  " +
        "avg(oven_heater_temp) - (3 * stddev(oven_heater_temp)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate, hour"
    )
  }

  def getDailyCookRecipeOvenHeaterStats(session: SparkSession, dfCookStart: DataFrame): DataFrame =  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Oven-Heater-Temperature Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, recipe_group ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, recipe_group, count(*) cnt, " +
        "avg(oven_heater_temp) avg_oven_heater_temp, stddev(oven_heater_temp) stdev_oven_heater_temp, " +
        "avg(oven_heater_temp) + (3 * stddev(oven_heater_temp)) UCL,  " +
        "avg(oven_heater_temp) - (3 * stddev(oven_heater_temp)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate, recipe_group"
    )
  }

  def getDailyCookRecipeDetailOvenHeaterStats(session: SparkSession, dfCookStart: DataFrame): DataFrame =  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Oven-Heater-Temperature Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, recipe_group,  recipe_subgroup ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, recipe_group,  recipe_subgroup, count(*) cnt, " +
        "avg(oven_heater_temp) avg_oven_heater_temp, stddev(oven_heater_temp) stdev_oven_heater_temp, " +
        "avg(oven_heater_temp) + (3 * stddev(oven_heater_temp)) UCL,  " +
        "avg(oven_heater_temp) - (3 * stddev(oven_heater_temp)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate, recipe_group,  recipe_subgroup"
    )
  }

  def getDailyCookRecipeQtyOvenHeaterStats(session: SparkSession, dfCookStart: DataFrame): DataFrame =  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Oven-Heater-Temperature Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, recipe_group,  recipe_subgroup, cook_qty ---------")
    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, recipe_group, recipe_subgroup, cook_qty, count(*) cnt, " +
        "avg(oven_heater_temp) avg_oven_heater_temp, stddev(oven_heater_temp) stdev_oven_heater_temp, " +
        "avg(oven_heater_temp) + (3 * stddev(oven_heater_temp)) UCL,  " +
        "avg(oven_heater_temp) - (3 * stddev(oven_heater_temp)) LCL  " +
        "from cookstart " +
        "group by cook_chamber, cook_type, eventdate, recipe_group,  recipe_subgroup, cook_qty"
    )
  }

  def getDeviceDailyCookOvenHeaterStats(session: SparkSession, dfCookStart: DataFrame): DataFrame=  {

    logger.info("  TurboChef MachineEvent (Cook-Start) Oven-Heater-Temperature Aggregator  ---")
    logger.info("Started processing: ---- group by cook_chamber, cook_type, eventdate, device_id ---------")

    dfCookStart.createOrReplaceTempView("cookstart")
    session.sqlContext.sql(
      "select  device_id, cook_chamber, cook_type, substring(eventtime, 0,10) eventdate, count(*) cnt, " +
        "avg(oven_heater_temp) avg_oven_heater_temp, stddev(oven_heater_temp) stdev_oven_heater_temp, " +
        "avg(oven_heater_temp) + (3 * stddev(oven_heater_temp)) UCL,  " +
        "avg(oven_heater_temp) - (3 * stddev(oven_heater_temp)) LCL  " +
        "from cookstart " +
        "group by device_id, cook_chamber, cook_type, eventdate"
    )
  }

}
