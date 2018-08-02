package com.jd.ad.anti.cps.dumper

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import com.jd.ad.anti.cps.ExecutorArgs

class AntiUnionClickLog extends DataDumper {
  override def saveData(sc: SparkContext, data: RDD[Map[String, Any]], argv: ExecutorArgs): Unit = {
    val columnNames = List(
      "union_id",
      "sub_union_id",
      "refer",
      "click_id",
      "ad_traffic_type",
      "click_time",
      "jda",
      "click_ip",
      "ua",
      "PREV_union_id",
      "PREV_click_time"
      )
    val tableName = argv.outputTable
    val basePartitionPath = argv.outputPath
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)
    val repartitionNum = 5
//    println("saveData count: " + data.count)

    val policyIds = data.map { row =>
      row.getOrElse("policy_id", "")
    }.distinct.collect

    policyIds.foreach { policyId =>
      val bcPolicyId = sc.broadcast(policyId)
      val reducedData = data.filter { row =>
        val policyId = bcPolicyId.value
        row.getOrElse("policy_id", "").equals(policyId)
      }
      val partitionPath = "%s/policy_id=%s".format(basePartitionPath, policyId)
      val partitions = "dt='%s', policy_id='%s'".format(date, policyId)
      saveAsTable(sc, reducedData, partitionPath, tableName, partitions, repartitionNum, columnNames)
    }
  }
}

