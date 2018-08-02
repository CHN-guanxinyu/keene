package com.jd.ad.anti.cps.dumper

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import com.jd.ad.anti.cps.ExecutorArgs

class AntiInnerOrderLog extends DataDumper {
  override def saveData(sc: SparkContext, data: RDD[Map[String, Any]], argv: ExecutorArgs): Unit = {
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
      "jda", 
      "click_id",
      "policy_id"
      )
    val tableName = argv.outputTable
    val partitionPath = argv.outputPath
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)
    val repartitionNum = 1
    //println("saveData count: " + data.count)

    val partitions = "dt='%s'".format(date)
    saveAsTable(sc, data, partitionPath,
      tableName, partitions, repartitionNum, columnNames)
  }
}
