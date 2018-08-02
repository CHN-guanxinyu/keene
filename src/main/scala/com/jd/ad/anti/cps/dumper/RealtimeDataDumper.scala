package com.jd.ad.anti.cps.dumper

import com.jd.ad.anti.cps.realtime.{CheckPoint, CmdExecutor, JobMonitor, Statistic}
import com.jd.ad.anti.cps.util.JobIni
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream


/**
  * Created by haojun on 2017/10/9.
  */
abstract class RealtimeDataDumper {
  def saveData(ssc:StreamingContext, dataStream: DStream[Map[String,Any]], conf:JobIni, statistic:Statistic): Unit

  def checkStopCmd(ssc:StreamingContext, dataStream: DStream[Map[String,Any]], conf:JobIni, statistic:Statistic): Unit = {

    val cmdExecutor = new CmdExecutor(conf, ssc)

    val dummyRdd = dataStream.transform((rdd: RDD[Map[String, Any]], t: Time) => {
      CheckPoint.touchDoneFile(conf, t.milliseconds - conf.getJobFrequency() * 1000)
      cmdExecutor.checkStopCmd()
      rdd
    })

    dummyRdd.foreachRDD(rdd => {
      statistic.endJob()
      if (conf.getMonitorEnabled()) {statistic.report()}
    })

  }
}
