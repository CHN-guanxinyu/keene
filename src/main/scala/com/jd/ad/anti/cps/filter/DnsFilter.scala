package com.jd.ad.anti.cps.filter

import org.apache.spark.rdd.RDD
import com.jd.ad.anti.cps.yaml._
import org.apache.spark.sql.Row

import scala.util.Try
import scala.util.control.Breaks

/**
  * Created by Jun on 2017/7/15.
  */
class DnsFilter(conf: _Filter) extends RuleFilter(conf) {

  override def doGroup(data: RDD[Map[String, Any]], 
    groupKey: String, 
    sortKey: String,
    clickTimeDiffRange: Map[String, Int],
    filterSpace: Space): RDD[Map[String, Any]] = {
    val sc = data.sparkContext
    val groupKeys = groupKey.split(",").map(_.trim)
    val bcGroupKeys = sc.broadcast(groupKeys)
    val bcClickTimeDiffRange = sc.broadcast(clickTimeDiffRange)

    if (isDebug) {
      println("group input count: " + data.count)
      println("group input rdd num: " + data.getNumPartitions)
      if (!data.isEmpty()) println(data.first)
    }
    
    val groupedData = data.map { row =>
      val mGroupKeys = bcGroupKeys.value
      val key = mGroupKeys.map {x => 
        val v = row.getOrElse(x,"")
        if (v == null) "" else v.toString
      }.mkString("|")
      (key, row)
    }.cache

    val billData = groupedData.groupByKey.flatMap { case (key, values) =>

      val rows = values.toList
      val clickTimeDiffRange = bcClickTimeDiffRange.value
      //clickContainer中保存相同click_seconds的所有点击中的union_id
      var clickContainer: Map[Long, Set[Long]] = Map()
      var fraudClickRows: Array[Map[String,Any]] = Array()


      def getStringAsLong(row: Map[String, Any], name: String, value: Long = 0L) : Long = {
        var returnValue: Long = value
        if (row.contains(name)){
          val v = row(name)
          if (v!=null) {
            Try (returnValue = v.asInstanceOf[Long])
          }
        }
        returnValue
      }

      rows.foreach { (r: Map[String,Any]) =>
        val click_seconds:Long = getStringAsLong(r,"click_seconds")
        val union_id:Long = getStringAsLong(r,"union_id")
        // click_seconds 有效才创建clickContainer元素
        if (click_seconds != 0L) {
          if (clickContainer.contains(click_seconds)) {
            var a: Set[Long] = clickContainer(click_seconds)
            a += union_id
            clickContainer -= click_seconds
            clickContainer += (click_seconds -> a)
          }else{
            clickContainer += (click_seconds -> Set(union_id))
          }
        }
      }

      rows.foreach { r =>
        val click_seconds:Long = getStringAsLong(r,"click_seconds")
        val union_id:Long = getStringAsLong(r,"union_id")

        val loop = new Breaks

        loop.breakable {
        //2个点击,10秒以内发生了union_id切换,判为作弊
          for (i <- clickTimeDiffRange("lower_seconds") to clickTimeDiffRange("upper_seconds")) {

            if (clickContainer.contains(click_seconds - i)) {
            //clickContainer(click_seconds-i).size>1,之前点击中必须包含其他union_id
              if (clickContainer(click_seconds - i).contains(union_id)
                && clickContainer(click_seconds - i).size > 1) {
                fraudClickRows :+= r
                loop.break
            //clickContainer(click_seconds-i).size=1,只包含其他union_id
              }else if(!clickContainer(click_seconds - i).contains(union_id)){
                fraudClickRows :+= r
                loop.break
              }
            }
          }
        }
      }

      fraudClickRows.toList

    }.cache


    if (isDebug) {
      println("dnsFilterData count: " + billData.count)
      if (!billData.isEmpty())
        println(billData.first)
    }
    billData
  }

}

