package com.jd.ad.anti.cps.filter

import com.jd.ad.anti.cps.util.JobIni
import com.jd.ad.anti.cps.variable.{RealtimeVariables}
import com.jd.ad.anti.cps.yaml._Filter
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

/**
  * Created by haojun on 2017/9/13.
  */
class RealtimeDnsFilter(conf: _Filter) extends DnsFilter(conf) {

  def init(ssc:StreamingContext, jobConf:JobIni, realVars:RealtimeVariables) = {
    vars = realVars.varsMap

    initRules()
  }

}
