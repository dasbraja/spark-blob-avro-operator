package starbucks.bits.siot.aggregator.heartbeat

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import starbucks.bits.siot.Session


object HeartbeatAggregator extends  LazyLogging{
  def getHourlyEdgeHBStats(session: SparkSession, df: DataFrame): DataFrame=  {
    logger.info("  Hourly Edge heartbeat Aggregator  ---")
    logger.info("Started processing: ---- group  by em_id, eventdate, eventhour ---------")
    df.createOrReplaceTempView("edgehb")
    session.sqlContext.sql(
      "select  em_id, substring(eventtime, 0,10) eventdate, substring(eventtime, 12,2) eventhour, " +
        "count(*) cnt, " +
        "max(eventtime) max_eventtime " +
        "from edgehb " +
        "group by em_id, eventdate, eventhour " +
        "having cnt < 19 and cnt > 1 "
    )
  }

  def getEmidListFromEdgeHB(session: SparkSession, df: DataFrame, filterDate: String) = {

    logger.info("Started processing: ---- group  by em_id ---------")
    df.createOrReplaceTempView("edgehb")
    session.sqlContext.sql(
      "select  em_id, substring(eventtime, 0,10) eventdate, " +
        "max(eventtime) latest_edgehb " +
        " from edgehb where substring(eventtime, 0,10) = '" + filterDate +
        "' group by em_id, eventdate "
    )

  }

  def getIrregularEdgeHBStats(session: SparkSession, df: DataFrame): DataFrame=  {
    logger.info("  Daily Irregular Edge heartbeat  Aggregator  ---")
    logger.info("Started processing: ---- group  by em_id, eventdate ---------")
    df.createOrReplaceTempView("edgehb")
    session.sqlContext.sql(
      "select  eventdate,  cnt hourly_hb_count, " +
        "count(*) cnt, count(distinct em_id) emid_count " +
        "from edgehb  group by eventdate, hourly_hb_count order by hourly_hb_count desc"
    )
  }

  def getHourlyEdgeHBDeviationStats(session: SparkSession, df: DataFrame): DataFrame=  {
    logger.info("  Daily Edge heartbeat deviation Aggregator  ---")
    logger.info("Started processing: ---- group  by em_id, eventdate ---------")
    df.createOrReplaceTempView("edgehb")
    session.sqlContext.sql(
      "select  em_id, eventdate, eventhour, cnt, " +
        "avg(cnt) over (partition by eventdate, eventhour) fleetAvg_Count, " +
        "max_eventtime " +
        "from edgehb "
    )
  }

  def getHourlyDeviceHBStats(session: SparkSession, df: DataFrame): DataFrame=  {
    logger.info("  Hourly Device heartbeat Aggregator  ---")
    logger.info("Started processing: ---- group  by deviceid, eventdate, eventhour ---------")
    df.createOrReplaceTempView("devicehb")
    session.sqlContext.sql(
      "select  device_id, substring(eventtime, 0,10) eventdate, substring(eventtime, 12,2) eventhour, " +
        "count(*) cnt, " +
        "max(eventtime) max_eventtime " +
        "from devicehb " +
        "group by device_id, eventdate, eventhour " +
        "having cnt < 19 and cnt > 1 "
    )
  }

  def getDuplicateGM(session: SparkSession, df: DataFrame): DataFrame=  {
    //logger.info("  Hourly Device heartbeat Aggregator  ---")
    logger.info("Started processing: Duplicate GMs---- group  by device_id ---------")
    df.createOrReplaceTempView("devicehb")
    session.sqlContext.sql(
      "select  device_id,  " +
        "count(distinct em_id) em_id_cnt " +
        "from devicehb " +
        "group by device_id  having em_id_cnt >1"
    )
  }

  def getDeviceGMMapping(session: SparkSession, df: DataFrame, filterDate: String): DataFrame = {
    logger.info("Started processing: Device GM mapping---- group  by device_id, em_id ---------")
    df.createOrReplaceTempView("devicehb")
    session.sqlContext.sql(
      "select  device_id, em_id, substring(eventtime, 0,10) eventdate, " +
        "max(eventtime) latest_devicehb " +
        " from devicehb where substring(eventtime, 0,10) = '" + filterDate +
        "' group by device_id, em_id, eventdate "
    )
  }

  def getDuplicateDevices(session: SparkSession, df: DataFrame): DataFrame=  {
    //logger.info("  Hourly Device heartbeat Aggregator  ---")
    logger.info("Started processing: Duplicate devicess---- group  by em_id ---------")
    df.createOrReplaceTempView("devicehb")
    session.sqlContext.sql(
      "select  em_id,  " +
        "count(distinct device_id) device_id_cnt " +
        "from devicehb " +
        "group by em_id  having device_id_cnt >1"
    )
  }

  def getIrregularDeviceHBStats(session: SparkSession, df: DataFrame): DataFrame=  {
    logger.info("  Daily Irregular Device heartbeat  Aggregator  ---")
    logger.info("Started processing: ---- group  by device_id, eventdate ---------")
    df.createOrReplaceTempView("devicehb")
    session.sqlContext.sql(
      "select  eventdate,  cnt hourly_hb_count, " +
        "count(*) cnt, count(distinct device_id) device_count  " +
        "from devicehb  group by eventdate, hourly_hb_count order by hourly_hb_count desc"
    )
  }

  def getDeviceDisconnectedEdgeConnected(session: SparkSession, dfEdge: DataFrame, dfDevice: DataFrame): DataFrame = {

    dfEdge.createOrReplaceTempView("edgehb")
    dfDevice.createOrReplaceTempView("devicehb")
    val dfEmList = session.sqlContext.sql(
      "select edgehb.em_id, edgehb.eventdate   " +
        "from edgehb " +
        "  left join devicehb " +
        " on edgehb.em_id = devicehb.em_id  " +
        " and edgehb.eventdate = devicehb.eventdate " +
        " where devicehb.em_id is null"
    )
    dfEmList.createOrReplaceTempView("emlist")
    val dfEmTransform = session.sqlContext.sql(
      "select em_id, cast(eventdate as date) eventdate, " +
        " coalesce(datediff(eventdate, lag(eventdate, 7) over(partition by em_id order by eventdate)),-1) as datediff7,  " +
        " coalesce(datediff(eventdate , lag(eventdate, 6) over(partition by em_id order by eventdate)), -1) as datediff6,  " +
        " coalesce(datediff(eventdate, lag(eventdate, 5) over(partition by em_id order by eventdate)),-1) as datediff5,  " +
        " coalesce(datediff(eventdate , lag(eventdate, 4) over(partition by em_id order by eventdate)),-1) as datediff4,  " +
        " coalesce(datediff(eventdate , lag(eventdate, 3) over(partition by em_id order by eventdate)), -1) as datediff3,  " +
        " coalesce(datediff(eventdate , lag(eventdate, 2) over(partition by em_id order by eventdate)), -1) as datediff2,  " +
        " coalesce(datediff(eventdate , lag(eventdate, 1) over(partition by em_id order by eventdate)), -1) as datediff1  " +
        "from emlist"
    )

    dfEmTransform.createOrReplaceTempView("emtransform")
    val dfDisconnectDevices = session.sqlContext.sql(
      "select em_id, eventdate,  " +
        "case " +
        " when datediff1 =1 and datediff2 =2 and datediff3 =3 and datediff4 =4 and datediff5 =5  and datediff6 =6 and datediff7 =7 then 8 " +
        " when datediff1 =1 and datediff2 =2 and datediff3 =3 and datediff4 =4 and datediff5 =5  and datediff6 =6 then 7 "  +
        " when datediff1 =1 and datediff2 =2 and datediff3 =3 and datediff4 =4 and datediff5 =5 then 6 "  +
        " when datediff1 =1 and datediff2 =2 and datediff3 =3 and datediff4 =4 then 5 "  +
        " when datediff1 =1 and datediff2 =2 and datediff3 =3 then 4 "  +
        " when datediff1 =1 and datediff2 =2 then 3 "  +
        " when datediff1 =1 then 2 " +
        " else 1 end running_days "  +
        " from emtransform "
    )
    dfDisconnectDevices
  }


  def getDevicesWithDisconnectedPeriod(session: SparkSession,
                                       df: DataFrame,
                                       disconnectPeriod: String):DataFrame = {

    df.createOrReplaceTempView("hb3")
    val sql1 = "select  message_type, em_id, substring(timestamp, 0,10) eventdate, substring(timestamp, 12,2) eventhour, " +
      "to_timestamp(timestamp) timestamp,  lag(to_timestamp(timestamp), 1) over(partition by em_id, message_type order by to_timestamp(timestamp)) lag_timestamp, " +
      " lag(message_type, 1) over(partition by em_id order by to_timestamp(timestamp)) lag_message_type, " +
      " last_value(to_timestamp(timestamp)) over(partition by message_type, em_id order by to_timestamp(timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) latest_selfhb_timestamp, " +
      " last_value(to_timestamp(timestamp)) over(partition by em_id order by to_timestamp(timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) latest_timestamp, " +
      " first_value(to_timestamp(timestamp)) over(partition by message_type, em_id order by to_timestamp(timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) earliest_selfhb_timestamp, " +
      "((bigint(to_timestamp(timestamp))) - (bigint(to_timestamp( lag(timestamp, 1) over(partition by em_id, message_type order by to_timestamp(timestamp)))))) self_hb_diff, " +
      "((bigint(to_timestamp(timestamp))) - (bigint(to_timestamp( lag(timestamp, 1) over(partition by em_id order by to_timestamp(timestamp)))))) any_hb_diff, " +
      "((bigint(to_timestamp( last_value(to_timestamp(timestamp)) over(partition by em_id order by to_timestamp(timestamp)  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )))) - (bigint(to_timestamp(last_value(to_timestamp(timestamp)) over(partition by message_type, em_id order by to_timestamp(timestamp)  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))))) disconnect_hb_diff " +
      " from hb3 "

    val df2 = session.sqlContext.sql( sql1)

    df2.createOrReplaceTempView("hb4")

    val sql2 = " select message_type, em_id, " +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then latest_timestamp else  timestamp end to_timestamp, " +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then timestamp else  lag_timestamp end from_timestamp, " +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then substring(timestamp,0,10) else  substring(lag_timestamp,0,10) end eventdate, " +
      "lag_message_type prior_message_type, earliest_selfhb_timestamp, latest_timestamp, latest_selfhb_timestamp," +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then disconnect_hb_diff else  self_hb_diff end self_hb_diff, " +
      "any_hb_diff, disconnect_hb_diff " +
      "from hb4 " +
      " where ( self_hb_diff > " + disconnectPeriod + " or any_hb_diff > " + disconnectPeriod + " or (disconnect_hb_diff > " + disconnectPeriod +" and timestamp = latest_selfhb_timestamp))"

    session.sqlContext.sql(sql2)
  }


  def getDevicesWithDisconnectedPeriod2(session: SparkSession,
                                       df: DataFrame,
                                       disconnectPeriod: String):DataFrame = {

    df.createOrReplaceTempView("hb3")
    val sql1 = "select  message_type, emid, substring(timestamp, 0,10) eventdate, substring(timestamp, 12,2) eventhour, " +
      "to_timestamp(timestamp) timestamp,  lag(to_timestamp(timestamp), 1) over(partition by emid, message_type order by to_timestamp(timestamp)) lag_timestamp, " +
      " lag(message_type, 1) over(partition by emid order by to_timestamp(timestamp)) lag_message_type, " +
      " last_value(to_timestamp(timestamp)) over(partition by message_type, emid order by to_timestamp(timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) latest_selfhb_timestamp, " +
      " last_value(to_timestamp(timestamp)) over(partition by emid order by to_timestamp(timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) latest_timestamp, " +
      " first_value(to_timestamp(timestamp)) over(partition by message_type, emid order by to_timestamp(timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) earliest_selfhb_timestamp, " +
      "((bigint(to_timestamp(timestamp))) - (bigint(to_timestamp( lag(timestamp, 1) over(partition by emid, message_type order by to_timestamp(timestamp)))))) self_hb_diff, " +
      "((bigint(to_timestamp(timestamp))) - (bigint(to_timestamp( lag(timestamp, 1) over(partition by emid order by to_timestamp(timestamp)))))) any_hb_diff, " +
      "((bigint(to_timestamp( last_value(to_timestamp(timestamp)) over(partition by emid order by to_timestamp(timestamp)  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )  ))) - (bigint(to_timestamp(last_value(to_timestamp(timestamp)) over(partition by message_type, emid order by to_timestamp(timestamp)  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) )))) disconnect_hb_diff " +
      //"((bigint(to_timestamp( last_value(to_timestamp(timestamp)) over(partition by emid)))) - (bigint(to_timestamp(last_value(to_timestamp(timestamp)) over(partition by message_type, emid))))) disconnect_hb_diff " +
      " from hb3 "

    val df2 = session.sqlContext.sql( sql1)

    df2.createOrReplaceTempView("hb4")

    val sql2 = " select message_type, emid, " +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then latest_timestamp else  timestamp end to_timestamp, " +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then timestamp else  lag_timestamp end from_timestamp, " +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then substring(timestamp,0,10) else  substring(lag_timestamp,0,10) end eventdate, " +
      "lag_message_type prior_message_type, earliest_selfhb_timestamp, latest_timestamp, latest_selfhb_timestamp," +
      "case when self_hb_diff < " + disconnectPeriod + " and disconnect_hb_diff > " + disconnectPeriod + " and timestamp = latest_selfhb_timestamp then disconnect_hb_diff else  self_hb_diff end self_hb_diff, " +
      "any_hb_diff, disconnect_hb_diff " +
      "from hb4 " +
      " where ( self_hb_diff > " + disconnectPeriod + " or any_hb_diff > " + disconnectPeriod + " or (disconnect_hb_diff > " + disconnectPeriod +" and timestamp = latest_selfhb_timestamp))"

    session.sqlContext.sql(sql2)
  }


}
