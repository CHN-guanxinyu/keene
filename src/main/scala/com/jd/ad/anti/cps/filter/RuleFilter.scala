package com.jd.ad.anti.cps.filter

import org.apache.spark.rdd.RDD
import com.jd.ad.anti.cps.yaml._

import scala.collection.mutable.ListBuffer

/**
  * Created by Jun on 2017/7/15.
  */
class RuleFilter(conf: _Filter) extends Filter(conf) {

  def isDebug = false

  def doFilter(data: RDD[Map[String, Any]]): RDD[Map[String, Any]] = {
    val sc = data.sparkContext

    if (isDebug) {
      println("filter input")
      if (!data.isEmpty()) println(data.first)
    }

    var billData: RDD[Map[String, Any]] = sc.emptyRDD
    val bcPolicyId = sc.broadcast(policyId)
    for ((space, (groupKey, sortKey, clickTimeDiffRange), filterSpace) <- rules) {
      val bcSpace = sc.broadcast(space)
      val bcFilterSpace = sc.broadcast(filterSpace)
      val filteredData = data.filter { row =>
        val mSpace = bcSpace.value
        mSpace.contains(row)
      }

      if (isDebug) {
        println("filted data")
        if (!filteredData.isEmpty()) println(filteredData.first)
      }

      var groupedData = filteredData
      if (groupKey != "" && filteredData.getNumPartitions != 0) {
        groupedData = doGroup(
          filteredData, groupKey, sortKey, clickTimeDiffRange, filterSpace)
      }
      //TODO 当击中一个filter条件时，就应该跳出循环
      billData = billData.union(groupedData.filter { row =>
        val mFilterSpace = bcFilterSpace.value
        mFilterSpace.contains(row)
      }.map { row =>
        val policyId = bcPolicyId.value
        row + ("policy_id" -> policyId)
      })
    }

    billData.cache
    if (isDebug) {
      println("billData count: " + billData.count)
    }
    billData
  }

  def doGroup(data: RDD[Map[String, Any]],
              groupKey: String,
              sortKey: String,
              clickTimeDiffRange: Map[String, Int],
              filterSpace: Space): RDD[Map[String, Any]] = {
    val sc = data.sparkContext
    val groupKeys = groupKey.split(",").map(_.trim)
    val sortKeys = sortKey.split(",").map(_.trim)
    val bcGroupKeys = sc.broadcast(groupKeys)
    val bcSortKeys = sc.broadcast(sortKeys)
    val bcFilterSpace = sc.broadcast(filterSpace)
    val bcSortGenerator = sc.broadcast(new SortFuncGenerator())

    if (isDebug) {
      println("group input count: " + data.count)
      println("group input rdd nums: " + data.getNumPartitions)
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

    var billData: RDD[Map[String, Any]] = sc.emptyRDD
    if (sortKeys.length != 0 && sortKeys(0) != "") {
      // group by groupKey and sort by sortKey
      // Row_1 => Row_1
      // Row_2 => Row_2 prev_* (prev = Row_1)
      billData = groupedData.groupByKey.flatMap { case (key, values) =>
        val rows = values.toList
        val sortKeys = bcSortKeys.value
        val sortGenerator = bcSortGenerator.value
        val newRows: ListBuffer[Map[String, Any]] =
          new ListBuffer[Map[String, Any]]()
        val sortedRows = rows.sortWith(sortGenerator.get(sortKeys))
        for(i <- 1 until sortedRows.length) {
          newRows.append(
            sortedRows(i) ++ sortedRows(i-1).map{case(_key, _value) => ("PREV_" + _key, _value)})
        }
        newRows
      }.cache
    }

    if (isDebug) {
      println("doGroupData count: " + billData.count)
      if (!billData.isEmpty())
        println(billData.first)
    }
    billData
  }

}

class SortFuncGenerator extends Serializable {

  def get(sortKeys: Array[String])
         (a: Map[String, Any] , b: Map[String, Any]): Boolean = {
    var compared = false
    var swap = false
    for( k <- sortKeys ) {
      if( a.getOrElse(k, "0") != null && b.getOrElse(k, "0") != null ) {
        val firstVal = a.getOrElse(k, "0")
        val secondVal = b.getOrElse(k, "0")
        if (firstVal.toString != secondVal.toString && !compared) {
          firstVal match {
            case x: String =>
              swap = x.toString < secondVal.toString
              compared = true
            case x: Int =>
              swap = x.asInstanceOf[Int] < secondVal.asInstanceOf[Int]
              compared = true
            case x: Long =>
              swap = x.asInstanceOf[Long] < x.asInstanceOf[Long]
              compared = true
            case _ =>
          }
        }
      }
    }
    swap
  }

}
