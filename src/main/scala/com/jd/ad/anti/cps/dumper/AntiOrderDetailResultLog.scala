package com.jd.ad.anti.cps.dumper

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class AntiOrderDetailResultLog extends DataDumper {
  override def saveData(sc: SparkContext, data: RDD[Map[String, Any]],argv: ExecutorArgs): Unit = {
    val columnNames = List(
      "id",
      "detail_id",
      "order_id",
      "order_time",
      "finish_time",
      "valid_flag",
      "comm_flag",
      "order_type",
      "platform",
      "spread_type",
      "cheat_type",
      "union_id",
      "sub_union_id",
      "pay_price",
      "commission_price",
      "anti_click_id",
      "site_id",
      "ad_traffic_type",
      "jda",
      "dt"
    )
    val tableName = argv.outputTable
    val basePartitionPath = argv.outputPath
    val repartitionNum = 5

    val dtArray = data.map { row =>
      row.getOrElse("dt", "")
    }.distinct.collect

    dtArray.foreach { dt =>
      val bcDt = sc.broadcast(dt)
      val reducedData = data.filter { row =>
        val dtValue = bcDt.value
        row.getOrElse("dt", "").equals(dtValue)
      }
      val partitionPath = "%s/dt=%s".format(basePartitionPath, dt)
      val partitions = "dt='%s'".format(dt)
      saveAsTable(sc, reducedData, partitionPath, tableName, partitions, repartitionNum, columnNames)
    }
  }
}
