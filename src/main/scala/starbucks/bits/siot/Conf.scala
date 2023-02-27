package starbucks.bits.siot

import com.typesafe.config.{Config, ConfigFactory}
import starbucks.bits.siot.Conf.config

import scala.io.Source

object Conf {
  val ehRecordSchema: String = Source.fromInputStream(getClass.getResourceAsStream("/EhRecord.avsc")).mkString
  val config: Config = ConfigFactory.load()
  val appName: String = config.getString("Env.spark.appname")
  //val logLevel: String = config.getString("main.logLevel")
  val sparkMaster: String = config.getString("Env.spark.masterurl")
  val executorMemory: String = config.getString("Env.spark.executormemory")
  val driverMemory: String = config.getString("Env.spark.drivermemory")
  //val executorNum: Int = config.getInt("Env.spark.executorNum")
  //val executorCores: Int = config.getInt("Env.spark.executorCores")

  val blobAccount: String = config.getString("Env.blob.account")
  val blobKey: String = config.getString("Env.blob.key")
  val blobFilePath: String = config.getString("AvroClient.path")

  val outblobAccount: String = config.getString("Env.blob.outaccount")
  val outblobKey: String = config.getString("Env.blob.outkey")
  val outblobFilePath: String = config.getString("AvroClient.outpath")

  val outblobAccount2: String = config.getString("Env.blob.outaccount2")
  val outblobKey2: String = config.getString("Env.blob.outkey2")

  val sysAttrCheck: String = config.getString("AvroClient.systemattr")
  val filter1RDD: String = config.getString("AvroClient.filter1")
  val filter2RDD: String = config.getString("AvroClient.filter2")
  val filter3RDD: String = config.getString("AvroClient.filter3")

  val batchInputPathPrefix: String = config.getString("AvroClient.batch.inputprefix")
  val batchInputPathSuffix: String = config.getString("AvroClient.batch.inputsuffix")
  val batchOutputPathPrefix: String = config.getString("AvroClient.batch.outputprefix")
  val batchOutputPathSuffix: String = config.getString("AvroClient.batch.outputsuffix")
  val batchDate: String = config.getString("AvroClient.batch.date")
  val batchHours: String = config.getString("AvroClient.batch.hours")
  val batchDates: String = config.getString("AvroClient.batch.dates")

  val batchavrojsonload = avrotojsonbatch(batchDate, batchHours,batchInputPathPrefix, batchInputPathSuffix, batchOutputPathPrefix, batchOutputPathSuffix)

  val hbInputPathPrefix: String = config.getString("HeartbeatAggregator.inputPathPrefix")
  val hbInputPathSuffix: String = config.getString("HeartbeatAggregator.inputPathSuffix")
  val edgeEmListPathPrefix: String = config.getString("HeartbeatAggregator.edgeEmListPathPrefix")
  val edgeHourlyAggPathPrefix: String = config.getString("HeartbeatAggregator.edgeHourlyAggPathPrefix")
  val deviceHourlyAggPathPrefix: String = config.getString("HeartbeatAggregator.deviceHourlyAggPathPrefix")
  val deviceEdgeMappingPathPrefix: String = config.getString("HeartbeatAggregator.deviceEdgeMappingPathPrefix")
  val hbAggregatedate: String = config.getString("HeartbeatAggregator.date")
  val hbAggConfig = hbaggconfig(hbAggregatedate, hbInputPathPrefix, hbInputPathSuffix, edgeEmListPathPrefix, edgeHourlyAggPathPrefix, deviceHourlyAggPathPrefix, deviceEdgeMappingPathPrefix)

  def setHbAggConfigMap(): hbaggconfigmap = {
    val filterDate = hbAggConfig.date.replaceAll("/","-")
    val inputPath = hbAggConfig.inputpathprefix + hbAggConfig.date + hbAggConfig.inputpathsuffix
    val edgeEmListPath = hbAggConfig.edgelistpathprefix + hbAggConfig.date
    val edgeHourlyAggPath = hbAggConfig.edgehourlyaggpathprefix + hbAggConfig.date
    val deviceHourlyAggPath = hbAggConfig.devicehourlyaggpathprefix + hbAggConfig.date
    val deviceEdgeMappingPath = hbAggConfig.deviceedgemappathprefix + hbAggConfig.date
    hbaggconfigmap(filterDate, inputPath, edgeEmListPath, edgeHourlyAggPath, deviceHourlyAggPath, deviceEdgeMappingPath)
  }

  def setHbAggConfigMap(inputDate: String): hbaggconfigmap = {
    val filterDate = inputDate.replaceAll("/","-")
    val inputPath = hbAggConfig.inputpathprefix + inputDate + hbAggConfig.inputpathsuffix
    val edgeEmListPath = hbAggConfig.edgelistpathprefix + inputDate
    val edgeHourlyAggPath = hbAggConfig.edgehourlyaggpathprefix + inputDate
    val deviceHourlyAggPath = hbAggConfig.devicehourlyaggpathprefix + inputDate
    val deviceEdgeMappingPath = hbAggConfig.deviceedgemappathprefix + inputDate
    hbaggconfigmap(filterDate, inputPath, edgeEmListPath, edgeHourlyAggPath, deviceHourlyAggPath, deviceEdgeMappingPath)
  }
  val disonnectAnalysisEdgeIputPath: String = config.getString("DisconnectAnalysis.edgeIputPath")
  val disonnectAnalysisDeviceIputPath: String = config.getString("DisconnectAnalysis.deviceIputPath")
  val disonnectAnalysisEdgeOnlyOutputPath: String = config.getString("DisconnectAnalysis.edgeOnlyOutputPath")
  val disconnectAnalysisConfig = disconnectconfig(disonnectAnalysisEdgeIputPath, disonnectAnalysisDeviceIputPath, disonnectAnalysisEdgeOnlyOutputPath)


  val hbFilterDevicesConfig = hbfilterdevices(
    config.getString("HeartbeatFilterDevices.dates"),
    config.getString("HeartbeatFilterDevices.inputPrefix"),
    config.getString("HeartbeatFilterDevices.inputSuffix"),
    config.getString("HeartbeatFilterDevices.outputPrefix"),
    config.getString("HeartbeatFilterDevices.outputSuffix"),
    config.getString("HeartbeatFilterDevices.attrValue"),
    config.getString("HeartbeatFilterDevices.attrKey")
  )

  def deviceDisonnectPeriodConfig: devicedisconnectperiod = {
    devicedisconnectperiod(
      config.getString("DevicesDisconnectPeriod.heartbeatPath"),
      config.getString("DevicesDisconnectPeriod.disconnectDurnSec"),
      config.getString("DevicesDisconnectPeriod.outputPath")
    )
  }

  def deviceTypeDisconnectPeriodConfig: devicetypedisconnectperiod = {
    devicetypedisconnectperiod (
      config.getString("DeviceTypeDisconnectPeriod.heartbeatPrefix"),
      config.getString("DeviceTypeDisconnectPeriod.disconnectDevicePath"),
      config.getString("DeviceTypeDisconnectPeriod.partitionkey"),
      config.getString("DeviceTypeDisconnectPeriod.disconnectDurnSec"),
      config.getString("DeviceTypeDisconnectPeriod.outputPath")
    )

  }

  def setHeartbeatFilterDevicetype: hbfilterdevicetype = {
    hbfilterdevicetype(
      config.getString("HeartbeatFilterDeviceType.emDeviceMapPath"),
      config.getString("HeartbeatFilterDeviceType.devicetypeMatchingExpr"),
      config.getInt("HeartbeatFilterDeviceType.outCollectionLimit"),
      config.getString("HeartbeatFilterDeviceType.dates"),
      config.getString("HeartbeatFilterDeviceType.attrKey"),
      config.getString("HeartbeatFilterDeviceType.inputPrefix"),
      config.getString("HeartbeatFilterDeviceType.inputSuffix"),
      config.getString("HeartbeatFilterDeviceType.outputPrefix"),
      config.getString("HeartbeatFilterDeviceType.outputSuffix")
    )
  }

  def setPartitionsByPartitionKey: devidepartitions = {
    devidepartitions(
      config.getString("PartitionByPartitionKey.inputpath"),
      config.getString("PartitionByPartitionKey.outputpath"),
      config.getString("PartitionByPartitionKey.partitionkey"),
      config.getString("PartitionByPartitionKey.partitionkeyCol")
    )
  }

}
