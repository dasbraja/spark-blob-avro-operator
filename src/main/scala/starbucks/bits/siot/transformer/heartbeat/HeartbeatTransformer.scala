package starbucks.bits.siot.transformer.heartbeat

import org.apache.spark.sql.DataFrame

object HeartbeatTransformer {

  def getEdgeHeartbeat(df: DataFrame) = {
    df
      .filter(
        df("message_type").equalTo("edge_heartbeat")
      ).selectExpr(
        "em_id",
      "timestamp as eventtime"
      )
  }

  def getDeviceHeartbeat(df: DataFrame) = {
    df
      .filter(
        df("message_type").equalTo("device_heartbeat")
      ).selectExpr(
      "device_id",
      "em_id",
      "timestamp as eventtime"
    )
  }


}
