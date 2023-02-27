package starbucks.bits.siot

case class EhRecord(SequenceNumber: Long,
                    Offset: String,
                    EnqueuedTimeUtc: String,
                    SystemProperties: Map[String, String],
                    Properties: Map[String, String],
                    Body: String)

case class avrotojsonbatch (date: String, hours: String, inpathprefix: String, inpathsuffix: String,
                                outpathprefix: String, outpathsuffix: String)

case class hbaggconfig (date: String, inputpathprefix: String, inputpathsuffix: String,
                        edgelistpathprefix: String, edgehourlyaggpathprefix: String,
                        devicehourlyaggpathprefix: String, deviceedgemappathprefix: String)

case class hbaggconfigmap (filterdate: String, inputpath: String, edgelistpath: String, edgehourlyaggpath: String,
                           devicehourlyaggpath: String, deviceedgemappath: String
                          )

case class disconnectconfig(edgeiputpath: String, deviceIputPath: String, edgeOnlyOutputPath: String)

case class hbfilterdevices(filterdates: String, inputprefix: String, inputsuffix: String,
                           outputprefix: String, outputsuffix: String, attrvalue: String, attrkey: String)

case class hbfilterdevicetype(emDeviceMapPath: String, devicetypeMatchingExpr: String, outCollectionLimit: Int,
                              dates: String, attrKey: String, inputprefix: String, inputsuffix: String,
                              outputprefix: String, outputsuffix: String)

case class devidepartitions(inputpath: String, outputpath: String, partitionkey: String, partitionkeycolumn: String)

case class devicedisconnectperiod(heartbeatpath: String, disconnectdurnsec: String, outputpath: String)

case class devicetypedisconnectperiod(heartbeatPrefix: String, disconnectDevicePath: String,
                                      partitionkey: String, disconnectDurnSec: String, outputPath: String)