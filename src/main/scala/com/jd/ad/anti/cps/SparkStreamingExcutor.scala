package com.jd.ad.anti.cps

import com.jd.ad.anti.cps.job.RealtimeJobExecutor
import org.slf4j.{Logger, LoggerFactory}
import com.jd.ad.anti.cps.util.JobIni
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.util.Try

object SparkStreamingExcutor {

  @transient
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getConfig(args : Array[String]) : JobIni = {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: $program <properties-file>")
    }
    new JobIni(args(0))
  }

  def main(args : Array[String]) : Unit = {
    val conf = getConfig(args)
    log.warn(conf.toString)

    val config = new SparkConf()
    val ssc = new StreamingContext(config, Seconds(conf.getJobFrequency()))

    RealtimeJobExecutor.run(ssc, conf)

    ssc.start()
    ssc.awaitTermination()
  }

}