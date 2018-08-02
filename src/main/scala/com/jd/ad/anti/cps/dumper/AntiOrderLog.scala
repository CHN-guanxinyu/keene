package com.jd.ad.anti.cps.dumper

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

import com.jd.ad.anti.cps.ExecutorArgs

class AntiOrderLog extends DataDumper {
  override def saveData(sc: SparkContext, data: RDD[Map[String, Any]],argv: ExecutorArgs): Unit = {
    val columnNames = List(
      "id",
      "detail_id",
      "order_id",
      "order_time",
      "ad_traffic_type",
      "platform",
      "union_id",
      "sub_union_id",
      "spread_type",
      "pay_price",
      "commission_price",
      "anti_click_id"
      )
    val tableName = argv.outputTable
    val basePartitionPath = argv.outputPath
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)
    val repartitionNum = 1

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
