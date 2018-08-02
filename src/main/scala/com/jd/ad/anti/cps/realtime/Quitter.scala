package com.jd.ad.anti.cps.realtime

import org.apache.spark.streaming.StreamingContext
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by zhujian on 2017/3/23.
  */
object Quitter {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def quit(seconds:Int, ssc:StreamingContext, msg:String):Unit = {
    log.error(msg)
    new TimeBomb(30)
    ssc.stop(true, false)
    log.error("Stop Spark Job!")
  }
}
