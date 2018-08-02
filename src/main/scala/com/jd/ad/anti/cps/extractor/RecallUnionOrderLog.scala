package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date

import com.jd.ad.anti.cps.ExecutorArgs

class RecallUnionOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    

    var querySql: String = s"""
         | select
         |     b.id as id,
         |     b.detail_id as detail_id,
         |     b.order_id as order_id,
         |     b.order_time as order_time,
         |     b.ad_traffic_type as ad_traffic_type,
         |     b.platform as platform,
         |     b.union_id as union_id,
         |     b.sub_union_id as sub_union_id,
         |     b.spread_type  as spread_type,
         |     b.pay_price as pay_price,
         |     b.sub_side_commission as commission_price,
         |     trim(b.sku_uuid) as anti_click_id,
         |     if(a.policy_id is null, "", a.policy_id) as policy_id,
         |     a.sub_union_cheat_num as sub_union_cheat_num
         | from
         |     (
         |        select union_id, sub_union_id, policy_id, platform, count(distinct id) sub_union_cheat_num
         |        from
         |            ${database}.cps_fraud_union_order
         |        where 
         |            dt = '${date}'
         |        group by union_id, sub_union_id, policy_id, platform
         |     )a
         |     join(
         |        select  *
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt = '${date}'
         |     )b
         |     on a.union_id = b.union_id and a.sub_union_id = b.sub_union_id and a.platform = b.platform
         |     where b.ad_traffic_type not in (17,142,196)
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val OrderLog = hiveContext.sql(querySql)
    val rows = OrderLog.rdd.map{ row =>
      Map(
          "id" -> row.getAs[Long]("id"),
          "detail_id" -> getStringOrElse(row, "detail_id"),
          "order_id" -> row.getAs[Long]("order_id"),
          "order_time" -> getStringOrElse(row, "order_time"),
          "ad_traffic_type" -> row.getAs[Int]("ad_traffic_type"),
          "platform" -> row.getAs[Int]("platform"),
          "union_id" -> row.getAs[Long]("union_id"),
          "sub_union_id" -> getStringOrElse(row, "sub_union_id"),
          "spread_type" -> row.getAs[Int]("spread_type"),
          "pay_price" -> row.getAs[Int]("pay_price"),
          "commission_price" -> row.getAs[Int]("commission_price"),
          "sub_union_cheat_num" -> row.getAs[Int]("sub_union_cheat_num"),
          "anti_click_id" -> getStringOrElse(row, "anti_click_id"),
          "policy_id" -> getStringOrElse(row, "policy_id")
      )
    }.cache
    val subUnionCounterMap = rows.map { row =>
      val key = "%s|%s|%s".format(
        row.getOrElse("union_id", 0).toString,
        row.getOrElse("sub_union_id", ""),
        row.getOrElse("platform",0).toString)
      val id = row.getOrElse("id", 0L)
      val idSet = Set(id)
      (key, idSet)
    }.reduceByKey { (a, b) =>
      a ++ b
    }.map{case(key, idSet) =>
      val subUnionKey = "sub_union_order_num"
      (key, Map(subUnionKey -> idSet.size))
    }.collectAsMap
    val bcSubUnionCounterMap = sc.broadcast(subUnionCounterMap)
    rows.map { row =>
        val subUnionCounterMap = bcSubUnionCounterMap.value
        val key = "%s|%s|%s".format(
          row.getOrElse("union_id", 0).toString,
          row.getOrElse("sub_union_id", ""),
          row.getOrElse("platform",0).toString)
        row ++ subUnionCounterMap.getOrElse(key, Map[String, Int]())
    }
  }
}
