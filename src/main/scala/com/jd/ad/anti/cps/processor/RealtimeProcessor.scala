package com.jd.ad.anti.cps.processor

import com.jd.ad.anti.cps.dumper.RealtimeDataDumper
import com.jd.ad.anti.cps.extractor.RealtimeDataExtractor
import com.jd.ad.anti.cps.realtime.{Statistic, JobMonitor}
import com.jd.ad.anti.cps.util.JobIni
import org.apache.spark.streaming.StreamingContext
import com.jd.ad.anti.cps.yaml._Job

/**
  * Created by haojun on 2017/9/13.
  */
abstract class RealtimeProcessor() {
  def process(ssc:StreamingContext, conf:JobIni, statistic:Statistic, extractor:RealtimeDataExtractor, dumper: RealtimeDataDumper)
}
