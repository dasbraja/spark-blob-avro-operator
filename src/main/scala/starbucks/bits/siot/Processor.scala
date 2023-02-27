package starbucks.bits.siot

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import starbucks.bits.siot.Conf.{deviceDisonnectPeriodConfig, deviceTypeDisconnectPeriodConfig, disconnectAnalysisConfig, hbFilterDevicesConfig, setHbAggConfigMap, setHeartbeatFilterDevicetype}
import starbucks.bits.siot.aggregator.heartbeat.HeartbeatAggregator
import starbucks.bits.siot.aggregator.turbochef.{CookStartAggregator, OvenStateAggregator}
import starbucks.bits.siot.transformer.heartbeat.HeartbeatTransformer
import starbucks.bits.siot.transformer.turbochef.MachineEventTransformer

object Processor extends LazyLogging {



  def processHeartbeatAggregate(inputDate: String) = {
    val session = Session()
    val hbAggConfigMap = setHbAggConfigMap(inputDate)

    val df = session.sqlContext.read.json(hbAggConfigMap.inputpath)
    logger.info(" input file loaded for " + inputDate)
    logger.info(" input file path " + hbAggConfigMap.inputpath)

    //     ---------- edge heartbeat aggregator ---------------------- //

    val dfEdgeHB = HeartbeatTransformer.getEdgeHeartbeat(df)
    logger.info(" edge hb loaded for " + inputDate)

    HeartbeatAggregator.getEmidListFromEdgeHB(session, dfEdgeHB, hbAggConfigMap.filterdate)
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(hbAggConfigMap.edgelistpath)

    logger.info(" emid list " + hbAggConfigMap.edgelistpath)
    logger.info(" emid list from edge hb loaded for " + inputDate)


    val dfEdgeHBHourlyAgg = HeartbeatAggregator.getHourlyEdgeHBStats(session, dfEdgeHB)
    dfEdgeHBHourlyAgg.filter(dfEdgeHBHourlyAgg("eventdate").equalTo(hbAggConfigMap.filterdate))
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(hbAggConfigMap.edgehourlyaggpath)

    logger.info(" edge hourly irregular hb loaded for " + inputDate)
    logger.info(" edge hourly aggregate path " + hbAggConfigMap.edgehourlyaggpath)
    //     ---------- end edge heartbeat aggregator ---------------------- //

    //     ---------- device heartbeat aggregator ---------------------- //

    val dfDeviceHB = HeartbeatTransformer.getDeviceHeartbeat(df)
    logger.info(" device hb loaded for " + inputDate)

    val dfDeviceHBHourlyAgg =HeartbeatAggregator.getHourlyDeviceHBStats(session, dfDeviceHB)


    dfDeviceHBHourlyAgg.filter(dfDeviceHBHourlyAgg("eventdate").equalTo(hbAggConfigMap.filterdate))
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(hbAggConfigMap.devicehourlyaggpath)

    logger.info(" device hourly irregular hb loaded for " + inputDate)
    logger.info(" device hourly aggregate path " + hbAggConfigMap.devicehourlyaggpath)

    HeartbeatAggregator.getDeviceGMMapping(session, dfDeviceHB, hbAggConfigMap.filterdate)
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(hbAggConfigMap.deviceedgemappath)

    logger.info(" device emid mapping list from device hb loaded for " + inputDate)
    logger.info(" device emid mapping list from device hb: " + hbAggConfigMap.deviceedgemappath)

    //     ---------- end device heartbeat aggregator ---------------------- //

    session.sparkContext.stop()

  }

  def processDeviceGMMapping(inputDate: String) = {
    val session = Session()
    val hbAggConfigMap = setHbAggConfigMap(inputDate)

    val df = session.sqlContext.read.json(hbAggConfigMap.inputpath)
    logger.info(" input file loaded for " + inputDate)
    logger.info(" input file path " + hbAggConfigMap.inputpath)

    //     ---------- device heartbeat aggregator ---------------------- //

    val dfDeviceHB = HeartbeatTransformer.getDeviceHeartbeat(df)
    logger.info(" device hb loaded for " + inputDate)

    HeartbeatAggregator.getDeviceGMMapping(session, dfDeviceHB, hbAggConfigMap.filterdate)
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(hbAggConfigMap.deviceedgemappath)

    logger.info(" device emid mapping list from device hb loaded for " + hbAggConfigMap.filterdate)
    logger.info(" device emid mapping list from device hb: " + hbAggConfigMap.deviceedgemappath)

    session.sparkContext.stop()
  }

  /* process daiy device disconnected edge connected sets */

  def processDisConnectedSets() = {
    val session = Session()

    val dfEdge = session.sqlContext.read.json(disconnectAnalysisConfig.edgeiputpath)
    val dfdevice = session.sqlContext.read.json(disconnectAnalysisConfig.deviceIputPath)

    logger.info(" input files loaded ")
    val dfDeviceDisconnectedEdgeConnected = HeartbeatAggregator.getDeviceDisconnectedEdgeConnected(session, dfEdge, dfdevice)

    logger.info(" tranformation complete: device disconnected , edge connected  ")
    dfDeviceDisconnectedEdgeConnected
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(disconnectAnalysisConfig.edgeOnlyOutputPath)

    logger.info(" output files loaded into blob store  ")

    session.sparkContext.stop()

  }

  def getDevicetypeEMList(emListPath: String,
                          matchExpr: String,
                          outCollectionLimit: Int): Seq[String] = {
    val session = Session()
    val dfEmList = session.sqlContext.read.json(emListPath)

    logger.info(" input files loaded " + emListPath)

    val em_list: Seq[String] = dfEmList.select( "device_id", "em_id")
      .filter(dfEmList("device_id")
        //.rlike(matchExpr))
      .startsWith(matchExpr))
      .selectExpr("em_id")
      .distinct()
      .rdd.take(outCollectionLimit)
      .map( x => x.toString()
        .replaceAll("\\[","")
        .replaceAll("]",""))
      .toList

    logger.info(" em_list created " )

    session.sparkContext.stop()
    em_list
  }

  def partitionWriteByKey(inputPath: String, outputPath: String,
                          partitionKey: String, partionKeyCol: String) = {
    val session = Session()
    val dfMaryChef = session.sqlContext.read.json(inputPath)
    logger.info(" input files loaded " + inputPath)
    dfMaryChef
      .withColumn(partitionKey, dfMaryChef(partionKeyCol))
      .repartition(dfMaryChef(partionKeyCol))
      .write.mode(SaveMode.Overwrite)
      .partitionBy(partionKeyCol)
      .json(outputPath)

    logger.info(" Partition write completed " + outputPath  + "  partition key:  " + partionKeyCol)
    logger.info(" Partition key:  " + partionKeyCol)
    session.sparkContext.stop()
  }

  def processDateRangeFilterByItemList(dates: Array[String],
                         itemList: Seq[String],
                         attrKey: String,
                         inputPathPrefix: String, inputPathSuffix: String,
                         outputPathPrefix: String, outputPathSuffix: String,
                        ) = {
    for(date<- dates) {
      val inputPath = inputPathPrefix + date + inputPathSuffix

      val outputPath = outputPathPrefix + date + outputPathSuffix
      val session = Session()
      val df3 = session.sqlContext.read.json(inputPath)
      df3.filter(df3(attrKey).isin(itemList:_*))
        .coalesce(4)
        .write.mode(SaveMode.Overwrite)
        .json(outputPath)

      session.sparkContext.stop()
      logger.info(s"---- Load completed for " + date + "---------------" )

    }
  }

  /* ---- filter  device heartbeats in date ranges  --- */
  def filterHeartbeatDateRange() = {
    val dates = hbFilterDevicesConfig.filterdates
    val dateArr = dates.split(";")
    val items = hbFilterDevicesConfig.attrvalue
    val itemList = items.split(";").toList
    val attrKey = hbFilterDevicesConfig.attrkey

    processDateRangeFilterByItemList(dateArr, itemList, attrKey,
      hbFilterDevicesConfig.inputprefix, hbFilterDevicesConfig.inputsuffix,
      hbFilterDevicesConfig.outputprefix, hbFilterDevicesConfig.outputsuffix
    )
  }

  def processHeartbeatByDeviceType() = {
    val em_list = Processor.getDevicetypeEMList (
      setHeartbeatFilterDevicetype.emDeviceMapPath,
      setHeartbeatFilterDevicetype.devicetypeMatchingExpr,
      setHeartbeatFilterDevicetype.outCollectionLimit
    )
    val dateArr = setHeartbeatFilterDevicetype.dates.split(";")
    Processor.processDateRangeFilterByItemList(
      dateArr, em_list,
      setHeartbeatFilterDevicetype.attrKey,
      setHeartbeatFilterDevicetype.inputprefix,
      setHeartbeatFilterDevicetype.inputsuffix,
      setHeartbeatFilterDevicetype.outputprefix,
      setHeartbeatFilterDevicetype.outputsuffix
    )
  }

  def getDevicesDisconnectedPeriod() = {
    val session = Session()
    val df = session.sqlContext.read.json(deviceDisonnectPeriodConfig.heartbeatpath)
    HeartbeatAggregator.getDevicesWithDisconnectedPeriod(session, df, deviceDisonnectPeriodConfig.disconnectdurnsec)
      .coalesce(4)
      .write.mode(SaveMode.Overwrite)
      .json(deviceDisonnectPeriodConfig.outputpath)

    session.sparkContext.stop()
  }

  def getDeviceTypeDisconnectedPeriod() = {

    /*---------- get em list for  device type(merrychef) devices ------------ */
    val em_listAll = Processor.getDevicetypeEMList (
      setHeartbeatFilterDevicetype.emDeviceMapPath,
      setHeartbeatFilterDevicetype.devicetypeMatchingExpr,
      setHeartbeatFilterDevicetype.outCollectionLimit
    )

    val session = Session()

    /*---------- get last 14 days disconnect em list for all devices ------------ */
    val emList = session.sqlContext.read.json(deviceTypeDisconnectPeriodConfig.disconnectDevicePath)
      .selectExpr(deviceTypeDisconnectPeriodConfig.partitionkey)
      .distinct()
      .rdd.take(5000)
      .map( x => x.toString()
        .replaceAll("\\[","")
        .replaceAll("]",""))
      .toList

    session.sparkContext.stop()

    /*---------- get last 14 days disconnect em list for device type(merrychef) devices ------------ */
    val em_intersect_list = em_listAll.intersect(emList)

    logger.info("disconnect list: " + emList.size + " \ndevicetype list: "
      + em_listAll.size + "\nintersect list: "
      + em_intersect_list.size
    )
    val groupedList: Seq[Seq[String]] = em_intersect_list.sliding(10,10).toList

    for(groups <-groupedList) {
      var emSuffix ="{"
      var hearbeatPath =""
      for(em <- groups) {
        emSuffix = emSuffix + deviceTypeDisconnectPeriodConfig.partitionkey + "="+em +","
      }
      emSuffix = emSuffix.substring(0,emSuffix.length-1) + "}"
      hearbeatPath = deviceTypeDisconnectPeriodConfig.heartbeatPrefix + emSuffix
      logger.info(hearbeatPath)
      val session2 = Session()
      val df = session2.sqlContext.read.json(hearbeatPath)
      HeartbeatAggregator.getDevicesWithDisconnectedPeriod2(session2, df,deviceTypeDisconnectPeriodConfig.disconnectDurnSec)
        .coalesce(4)
        .write.mode(SaveMode.Append)
        .json(deviceTypeDisconnectPeriodConfig.outputPath)
      session2.sparkContext.stop()
    }
  }


  def processTCOvenStateAggregate(session: SparkSession, df: DataFrame) = {
    val dfOvenState = MachineEventTransformer.getOvenState(df)

    OvenStateAggregator.getDailyOvenStateAgg(session, dfOvenState).show()
    OvenStateAggregator.getHourlyOvenStateAgg(session, dfOvenState).show()
    OvenStateAggregator.getDeviceOvenStateAgg(session, dfOvenState).show()
  }

  def processTCCookStartAggregate(session: SparkSession, df: DataFrame) = {
    val dfCookStart = MachineEventTransformer.getCookStart(df)

    dfCookStart.createOrReplaceTempView("cookstart")

    CookStartAggregator.getDailyCookDurationStats(session, dfCookStart).show()
    CookStartAggregator.getHourlyCookDurationStats(session, dfCookStart).show()
    CookStartAggregator.getDailyRecipeCookDurationStats(session, dfCookStart).show()
    CookStartAggregator.getDailyRecipeDetailCookDurationStats(session, dfCookStart).show()
    CookStartAggregator.getDailyRecipeQtyDetailCookDurationStats(session, dfCookStart).show()
    CookStartAggregator.getDeviceDailyCookDurationStats(session, dfCookStart).show()

    CookStartAggregator.getDailyCookOvenHeaterStats(session, dfCookStart).show()
    CookStartAggregator.getHourlyCookOvenHeaterStats(session, dfCookStart).show()
    CookStartAggregator.getDailyCookRecipeOvenHeaterStats(session, dfCookStart).show()
    CookStartAggregator.getDailyCookRecipeDetailOvenHeaterStats(session, dfCookStart).show()
    CookStartAggregator.getDailyCookRecipeQtyOvenHeaterStats(session, dfCookStart).show()
    CookStartAggregator.getDeviceDailyCookOvenHeaterStats(session, dfCookStart).show()
  }

}
