package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date

import com.jd.ad.anti.cps.ExecutorArgs

class RiskOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    

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
         |     a.cheat_type as cheat_type
         | from
         |     (
         |        select order_id, sku_id, order_time,union_id, cheat_type
         |        from
         |            app.app_jxj_risk_order
         |        where 
         |            dt = '${date}' and dp = 'cps'
         |        group by order_id, sku_id, order_time, union_id, cheat_type
         |     )a
         |     join(
         |        select  *
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt = '${date}'
         |     )b
         |     on a.order_id = b.order_id and a.sku_id = b.sku_id and a.order_time = b.order_time and a.union_id = b.union_id
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val orderLog = hiveContext.sql(querySql)
    orderLog.rdd.map{ row =>
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
          "anti_click_id" -> getStringOrElse(row, "anti_click_id"),
          "cheat_type" -> row.getAs[Int]("cheat_type")
      )
    }.distinct.cache
  }
}
