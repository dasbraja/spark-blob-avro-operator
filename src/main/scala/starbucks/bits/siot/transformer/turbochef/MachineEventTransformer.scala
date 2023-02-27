package starbucks.bits.siot.transformer.turbochef

import org.apache.spark.sql.DataFrame

object MachineEventTransformer {

  def getOvenState(df: DataFrame): DataFrame = {
    df
      .filter(
        df("body.body.event").equalTo("op")
          &&  df("body.body.op_id").equalTo("oven_state")
      ).selectExpr(
      "body.body.event as event",
      "body.body.op_id as op_id",
      "body.body.c as cook_chamber",
      "body.body.state as state",
      "body.type as message_type",
      "body.version as version",
      "body.device_type as device_type",
      "device_id as device_id",
      "em_id as em_id",
      "timestamp as eventtime"
    )
  }

  def getCookStart(df: DataFrame) = {
    df
      .filter(
        df("body.body.event").equalTo("op")
          &&  df("body.body.op_id").equalTo("cook_start")
      ).selectExpr(
      "body.body.event as event",
      "body.body.op_id as op_id",
      "body.body.c as cook_chamber",
      "body.body.cktime as cook_durn",
      "body.body.cktype as cook_type",
      "body.body.g as recipe_group",
      "body.body.sg as recipe_subgroup",
      "body.body.i as cook_qty",
      "body.body.sp_hx as oven_set_point",
      "body.body.t_hx as oven_heater_temp",
      "body.type as message_type",
      "body.version as version",
      "body.device_type as device_type",
      "device_id as device_id",
      "em_id as em_id",
      "timestamp as eventtime"
    )

  }

  def getCookPauseResume(df: DataFrame) = {
    df
      .filter(
        df("body.body.event").equalTo("op")
          &&  df("body.body.op_id").isin("cook_resume", "cook_pause")
      ).selectExpr(
      "body.body.event as event",
      "body.body.op_id as op_id",
      "body.body.c as cook_chamber",
      "body.body.r as reason",
      "body.body.sp_hx as oven_set_point",
      "body.body.t_hx as oven_heater_temp",
      "body.body.t_rem as cook_time_remaining",
      "body.type as message_type",
      "body.version as version",
      "body.device_type as device_type",
      "device_id as device_id",
      "em_id as em_id",
      "timestamp as eventtime"
    )

  }


  def getSysEvent(df: DataFrame) = {
    df
      .filter(
        df("body.body.event").equalTo("sys")
          &&  df("body.body.event_id").isin("enter_info", "leave_info")

      )
      .selectExpr(
        "body.body.event as event",
        "body.body.event_id as event_id",
        "body.type as message_type",
        "body.version as version",
        "body.device_type as device_type",
        "device_id as device_id",
        "em_id as em_id",
        "timestamp as eventtime"
      )
  }
  def getPowerConsumptionReport(df: DataFrame) = {
    df
      .filter(
        df("body.body.event").equalTo("sys")
          &&  df("body.body.event_id").equalTo("pwr_rpt")
      )
      .selectExpr(
        "body.body.event as event",
        "body.body.event_id as event_id",
        "body.body.avg_wav_cur as daily_avg_peak_current",
        "body.body.day_volt_sag_ct as daily_voltage_sag_observed_cnt",
        "body.body.day_volt_spike_ct as daily_voltage_spike_observed_cnt",
        "body.body.max_volts as daily_max_voltage_observed",
        "body.body.min_volts as daily_min_voltage_observed",
        "body.body.tot_energy as daily_total_energy_used",
        "body.type as message_type",
        "body.version as version",
        "body.device_type as device_type",
        "device_id as device_id",
        "em_id as em_id",
        "timestamp as eventtime"
      )
      //.toJSON.take(df.count.toInt).foreach(println)
  }

}
