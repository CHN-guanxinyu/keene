package com.jd.ad.anti.cps.realtime

import java.io.{IOException, FileNotFoundException}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import com.jd.ad.anti.cps.util.JobIni
import com.jd.ad.anti.cps.util.{FileUtils}
import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.streaming.kafka.OffsetRange

/**
  * Created by liujun on 2017/1/6.
  */
object CheckPoint {
  val log: Logger = LoggerFactory.getLogger(CheckPoint.getClass)

  val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  val doneFile = "_SUCCESS"
  val statusFile = "job_info.json"

  def restoreStatus(config: JobIni): Status = {
    val path = getLastPath(config.getCheckPointPath())
    if (path == null) {
      log.warn("No restore path, start from kafka earliest offset")
      Status()
    } else {
      log.warn("restoring status from:" + path)
      val jsonText = FileUtils.readTextFile(path + "/" + statusFile)
      Status().fromJson(jsonText)
    }
  }

  def storeStatus(config: JobIni, offsetRange: Array[OffsetRange], ms: Long): Unit = {
    val status = Status(offsetRange, ms)
    val path = getPath(config.getCheckPointPath(), ms)
    FileUtils.writeTextFile(path + "/" + statusFile, status.toJson())
   // FileUtils.touchDoneFile(path + "/" + doneFile)
    log.warn("store status to:" + path)
    deleteHistoryStatus(config.getCheckPointPath())
  }

  def touchDoneFile(config: JobIni, ms: Long): Unit = {
    val path = getPath(config.getCheckPointPath(), ms)
    FileUtils.touchDoneFile(path + "/" + doneFile)
  }

  def getPath(path: String, ms: Long): String = {
    path + "/" + dateFormat.format(new Date(ms))
  }

  def getLastPath(root: String): String = {
    val pat = """.*/20\d+""".r
    FileUtils.getLatestPath(root, (f: String) =>
      FileUtils.exists(f + "/" + doneFile) && pat.findFirstIn(f).isDefined
    )
  }

  def deleteHistoryStatus(root: String) = {
    val pat = """.*/20\d+""".r
    val fs = new Path(root).getFileSystem(new Configuration)
    val sortedFiles = fs.listStatus(new Path(root)).map(_.getPath.toString).sortWith(_.compareTo(_)>0)
    val CheckPointNum = 10
    if (sortedFiles.length > CheckPointNum) {
      var num = 0
      for (file <- sortedFiles) {
        if (FileUtils.exists(file + "/" + doneFile) && pat.findFirstIn(file).isDefined && num > CheckPointNum) {
          log.warn("delete path:" + file)
          FileUtils.delete(file)
        } else {
          num += 1
        }
      }
    }
  }

}
