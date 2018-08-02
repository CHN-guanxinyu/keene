package com.jd.ad.anti.cps.realtime

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}

/*
JobCounter类用来统计一个job的运行时间和处理数据量，以及错误信息
 */
class JobCounter(jobName: String, sc: SparkContext) extends Serializable {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  /* app total metric */
  private val jobTimer = new LocalTimer
  val totalCost: LongAccumulator = sc.longAccumulator(jobName + "_cost")
  val totalRecords: LongAccumulator = sc.longAccumulator(jobName + "_records")
  val totalDelay: LongAccumulator = sc.longAccumulator(jobName+"_delay")  //单位秒
  var avgDelay:Long = 0l
  private val jobErrorMsg = sc.collectionAccumulator[String]("jobErrorMsg")

  def startJob():Unit = {
    jobTimer.reset()
    totalCost.reset()
    totalRecords.reset()
    totalDelay.reset()
    avgDelay = 0l
    jobErrorMsg.reset()
  }

  def endJob():Unit = {
    freezeTotalCost()
    getAvgDelay
    reportJobMetric(totalCost.value, totalRecords.value, avgDelay)
  }

  def reportJobMetric(cost:Long, records:Long, avgDelay:Long):Unit= {
    log.warn("Job-%s Metric: cost=%.2f records=%d avgDelay=%d ErrorMsgNum=%d".format(
      jobName, cost/1000.0, records, avgDelay, jobErrorMsg.value.size()
    ))

    if(!jobErrorMsg.value.isEmpty){
      // todo: add the alarm
      log.error("Job-%s Encounter errors in processing: %s".format(jobName, jobErrorMsg.value.toString))
    }
  }

  def updateErrorMessage(msg:String):Unit = {
    jobErrorMsg.add(msg)
  }

  def updateTotalRecords(cnt: Long): Unit = {
    totalRecords.add(cnt)
  }

  def freezeTotalCost(): Unit = {
    totalCost.add(jobTimer.cost())
  }

  def updateTotalDelay(cnt: Long): Unit = {
    totalDelay.add(cnt)
  }

  def getTotalCost: Long = totalCost.value

  def getTotalRecords: Long = totalRecords.value

  def getTotalDelay: Long = totalDelay.value

  def getAvgDelay: Long = {
    if (totalDelay.value > 0 && totalRecords.value > 0){
      avgDelay = totalDelay.value/totalRecords.value
    }
    avgDelay
  }

  def clean(): Unit = {
    totalCost.reset()
    totalRecords.reset()
    totalDelay.reset()
    avgDelay = 0l
    jobTimer.reset()
    jobErrorMsg.reset()
  }

}

