package com.jd.ad.anti.cps.extractor

import com.jd.ad.anti.cps.realtime.{CheckPointHolder, Statistic}
import com.jd.ad.anti.cps.util.JobIni
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.util.Try

/**
  * Created by haojun on 2017/9/13.
  */
abstract class RealtimeDataExtractor extends Serializable {
  def getDStream(ssc:StreamingContext, conf:JobIni, statistic:Statistic) : DStream[Map[String,Any]]

  def getStringOrElse(v: String, defaultValue: String = "") : String = {
    if (v == null || v.toLowerCase == "null") defaultValue else v
  }

  def getStringAsLong(v: String, defaultValue: Long = 0L) : Long = {
    if ((v == null) || Try(v.toLong).isFailure) defaultValue else v.toLong
  }

  def getStringAsInt(v: String, defaultValue: Int = 0) : Int = {
    if ((v == null) || Try(v.toInt).isFailure) defaultValue else v.toInt
  }
}
