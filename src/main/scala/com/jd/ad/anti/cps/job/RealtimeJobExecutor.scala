package com.jd.ad.anti.cps.job

import com.jd.ad.anti.cps.dumper.{RealtimeAntiClickLog, RealtimeAntiOrderDumper, RealtimeDataDumper}
import com.jd.ad.anti.cps.extractor.{RealtimeDataExtractor, RealtimeUnionClickKafka}
import com.jd.ad.anti.cps.processor.{RealtimeClickProcessor, RealtimeOrderProcessor, RealtimeProcessor}
import com.jd.ad.anti.cps.realtime.{JobCounter, JobMonitor, Statistic}
import com.jd.ad.anti.cps.util.JobIni
import com.jd.ad.anti.cps.yaml.CpsAntiYaml
import org.apache.spark.streaming.StreamingContext
import org.slf4j.{Logger, LoggerFactory}
import com.jd.ad.anti.cps.extractor.RealtimeOrderMessageKafka
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by haojun on 2017/9/7.
  */
object RealtimeJobExecutor {
  @transient
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def run(ssc:StreamingContext, conf:JobIni): Unit = {

    CpsAntiYaml.initialize(conf.getYamlFile())

    //val counter:JobCounter = new JobCounter(conf.getJobName(), ssc.sparkContext)
    var statistic:Statistic = null

    var extractor:RealtimeDataExtractor = null
    var processor:RealtimeProcessor = null
    var dumper:RealtimeDataDumper = null

    if (conf.getJobName() == "RealtimeClickLogFilterJob") {
      extractor = new RealtimeUnionClickKafka()
      processor = new RealtimeClickProcessor()
      dumper = new RealtimeAntiClickLog()
      statistic = new Statistic("realtime_click", ssc.sparkContext, conf)
    } else if (conf.getJobName() == "OrderJoinClickLogFilterJob") {
      extractor = new RealtimeOrderMessageKafka()
      processor = new RealtimeOrderProcessor()
      dumper = new RealtimeAntiOrderDumper()
      statistic = new Statistic("realtime_order", ssc.sparkContext, conf)
    }

    processor.process(ssc, conf, statistic, extractor, dumper)


  }

  def getCount(offset: Array[OffsetRange]): Long = {
    offset.map(f => {
      f.untilOffset - f.fromOffset
    }).sum
  }
}
