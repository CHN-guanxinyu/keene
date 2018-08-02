package com.jd.ad.anti.cps.variable

import java.text.SimpleDateFormat

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext

abstract class AbstractVariable extends Serializable{
  def init(sc: SparkContext,argv: ExecutorArgs): Unit
  def updateData(sc:SparkContext, path:String, day:String): Unit
  def contains(args: List[String]): Boolean
  def containKey(args: List[String]): Boolean
  final def getRunDay(argv: ExecutorArgs): String={
    val day = argv.day
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val run_day = dateFormat.format( day )
    run_day
  }
  final def isKeyValid(day:String, start_time:String, end_time:String): Boolean={
    if (day >= start_time && day <= end_time) {
      true
    } else {
      false
    }
  }
}
