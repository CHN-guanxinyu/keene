package com.jd.ad.anti.cps.realtime

import com.jd.ad.anti.cps.util.JobIni
import com.jd.ad.anti.cps.util.DXMonitor
import org.slf4j.{LoggerFactory, Logger}
import java.sql.Timestamp

/**
 * Created by liujun19 on 2017/1/6.
 */


class JobMonitor(statistic: Statistic, conf: JobIni) extends Serializable {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  @transient
  val dxMonitor: DXMonitor = if (conf == null || !conf.getMonitorStatus()) null
  else
    new DXMonitor(
      conf.getMonitorUrl(),
      conf.getMonitorDb(),
      conf.getMonitorTable(),
      conf.getMonitorFields(),
      conf.getMonitorUser(),
      conf.getMonitorToken())

  def getStatistic():Statistic = statistic

  def reportInfo(): Unit = {
    printMetric()
    reportMetric(conf.getJobName())
    //counter.clean()
  }

  private def report(APP:String, tag: String, k:String, v: String): Unit = {
    if (dxMonitor == null ) {
      return
    }

    try {
      val timestamp = new Timestamp(System.currentTimeMillis())
      val values = Array[String](APP, tag, k, v)
      dxMonitor.report(timestamp, values)
    } catch {
      case e: Exception => {
        log.warn("Failed to report metrics, caused by:" + e.getCause)
      }
    }
  }

  def reportMetric(app:String):Unit = {
    statistic.getCounters.map{case (name, counter) =>
      report(app, name, "total_cost", counter.getTotalCost.toString)
      report(app, name, "total_records", counter.getTotalRecords.toString)

      if (name.equals("click_job") || name.equals("order_job"))
        report(app, name, "avg_delay", counter.getAvgDelay.toString)

      counter
    }
  }

  def printMetric():Unit = {
    statistic.getCounters.map{ case (name, counter) =>
      info(name + " total cost ", counter.getTotalCost)
      info(name + " total records", counter.getTotalRecords)
      info(name + " avg_delay", counter.getAvgDelay)
      counter
    }
  }

  def info(k:String, v:Long):Unit = {
    log.warn("------->%-12s:%d".format(k,v))
  }
}
