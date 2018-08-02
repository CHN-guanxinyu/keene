package com.jd.ad.anti.cps.realtime

import org.apache.spark.streaming.kafka.OffsetRange


/**
  * Created by haojun on 2017/9/11.
  */
class CheckPointHolder {
  var clkOffsetRange: Array[OffsetRange] = null


}
