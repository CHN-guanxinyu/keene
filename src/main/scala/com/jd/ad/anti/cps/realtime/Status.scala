package com.jd.ad.anti.cps.realtime

import java.text.SimpleDateFormat
import java.util.Date

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
//import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.common.TopicAndPartition


/**
  * Created by zhujian on 2016/11/25.
  */
case class Status(
                   offsetRangeList: Array[OffsetRange] = Array.empty,
                   batchTime: Long = 0
                 ) {
  def toJson(): String = {
    implicit lazy val formats = DefaultFormats
    Serialization.write(this)
  }

  def fromJson(json: String): Status = {
    implicit lazy val formats = DefaultFormats
    Serialization.read[Status](json)
  }

  /**
   * 3号集群
   * def getKafkaOffsets(topicSet:Set[String]): Map[TopicPartition, Long] = {
   * offsetRangeList.filter(x=>topicSet.contains(x.topic))
   * .map(x => (x.topicPartition(), x.untilOffset)).toMap
   * }
   */
  
  /** 
   *  1.2号集群
   */
  def getKafkaOffsets(topicSet: Set[String]): Map[TopicAndPartition, Long] = {
    offsetRangeList.filter(x => topicSet.contains(x.topic)).
    map(x => (x.topicAndPartition(), x.untilOffset)).toMap 
  }

  def getBatchTime(): String = {
    new SimpleDateFormat().format(new Date(batchTime))
  }
}
