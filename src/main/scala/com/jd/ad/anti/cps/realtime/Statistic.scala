package com.jd.ad.anti.cps.realtime

import com.jd.ad.anti.cps.util.JobIni
import org.apache.spark.SparkContext

class Statistic(app: String, sc: SparkContext, conf: JobIni) extends Serializable {
  var counters: Map[String, JobCounter] = init(app, sc)
  val monitor:JobMonitor = new JobMonitor(this, conf)
  val jobName: String = app
  
  def init(app: String, sc:SparkContext): Map[String, JobCounter] = {
    var cnts: Map[String, JobCounter] = Map()
    if ("realtime_click" == app) {
      cnts += ("click_job" -> new JobCounter("click_job", sc))
      cnts += ("click_301" -> new JobCounter("click_301", sc))
    } else if("realtime_order" == app) {
      cnts += ("300" -> new JobCounter("300", sc))
      cnts += ("301" -> new JobCounter("301", sc))
      cnts += ("307" -> new JobCounter("307", sc))
      cnts += ("308" -> new JobCounter("308", sc))
      cnts += ("order_job" -> new JobCounter("order_job", sc))
      cnts += ("fraud_orders" -> new JobCounter("fraud_orders", sc))
    }
    cnts
  }

  def startJob():Unit = {
    counters.foreach{ case (name,counter) =>
      this.startJob(name)
    }
  }

  def endJob():Unit = {
    counters.foreach{ case (name,counter) =>
      this.endJob(name)
    }
  }

  def startJob(name: String):Unit = {
    if( counters.get(name).isDefined) {
      counters(name).startJob()
    }
  }

  def endJob(name: String):Unit = {
    if( counters.get(name).isDefined) {
      counters(name).endJob()
    }
  }

  def report():Unit = {
    monitor.reportMetric(jobName)
  }

  def updateTotalRecords(name:String, cnt:Long): Unit ={
    if( counters.get(name).isDefined) {
      counters(name).updateTotalRecords(cnt)
    }
  }

  def freezeTotalCost(name:String): Unit ={
    if( counters.get(name).isDefined) {
      counters(name).freezeTotalCost()
    }
  }

  def updateTotalDelay(name:String, cnt:Long): Unit ={
    if( counters.get(name).isDefined) {
      counters(name).updateTotalDelay(cnt)
    }
  }

  def updateErrorMessage(name:String, msg:String): Unit ={
    if( counters.get(name).isDefined) {
      counters(name).updateErrorMessage(msg)
    }
  }

  def getCounters:Map[String, JobCounter] = counters

  def clean(): Unit = {
    counters.foreach{ case (name,counter) =>
      counter.clean()
    }
    counters = Map()
  }
}

