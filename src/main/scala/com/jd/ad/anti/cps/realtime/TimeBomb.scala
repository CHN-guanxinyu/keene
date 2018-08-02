package com.jd.ad.anti.cps.realtime

import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by zhujian on 2017/3/23.
  */
class TimeBomb(seconds:Int) {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  haltJVMAfter(seconds)

  private def haltJVMAfter(seconds:Int):Unit = {
    log.error("Will halt JVM after %d seconds".format(seconds))
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(seconds*1000)
        Runtime.getRuntime().halt(1)
      }
    }).start()
  }
}

