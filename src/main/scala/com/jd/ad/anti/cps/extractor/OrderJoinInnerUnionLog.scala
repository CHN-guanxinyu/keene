package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date

import com.jd.ad.anti.cps.ExecutorArgs

class OrderJoinInnerUnionLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    
    //hiveContext.sql("use fdm")

    var querySql: String = s"""
         |-- 通过jda,user_pin找到同一用户2天点击
         |select 
         |    id,
         |    detail_id,
         |    order_id,
         |    order_time,
         |    ad_traffic_type,
         |    platform,
         |    union_id,
         |    sub_union_id,
         |    spread_type,
         |    pay_price,
         |    commission_price,
         |    click_id,
         |    jda,
         |    union_id_start,
         |    request_tm_diff,
         |    order_request_tm_diff,
         |    policy_id
         |from
         |(
         | select
         |     a.id as id,
         |     a.detail_id as detail_id,
         |     a.order_id  as order_id,
         |     a.order_time as order_time,
         |     a.ad_traffic_type as ad_traffic_type,
         |     a.platform as platform,
         |     b.union_id as union_id,
         |     trim(a.sub_union_id) as sub_union_id,
         |     a.spread_type as spread_type,
         |     a.pay_price   as pay_price,
         |     a.sub_side_commission as commission_price,
         |     trim(a.sku_uuid)  as  click_id,
         |     b.jda  as jda,
         |     b.PREV_union_id as union_id_start,
         |     b.request_tm_diff as request_tm_diff,
         |     (unix_timestamp(a.order_time) - unix_timestamp(b.request_tm)) as order_request_tm_diff,
         |     if(b.policy_id is null, "", b.policy_id) as policy_id
         | from
         |     (
         |        select if(jda is null, "", jda) jda_, *
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt = '${date}' and order_time >= '${date}'
         |     ) a
         |     join
         |     (
         |        select *, unix_timestamp(request_tm) - unix_timestamp(PREV_request_tm) request_tm_diff
         |        from 
         |            ${database}.cps_fraud_inner_union
         |        where
         |            dt >= '${beginDate}'
         |            and
         |            dt <= '${date}'
         |     ) b
         |     on  a.union_id = b.union_id and a.jda_ = b.jda
         |
         |    union all
         |
         | select
         |     a.id as id,
         |     a.detail_id as detail_id,
         |     a.order_id  as order_id,
         |     a.order_time as order_time,
         |     a.ad_traffic_type as ad_traffic_type,
         |     a.platform as platform,
         |     b.union_id as union_id,
         |     trim(a.sub_union_id) as sub_union_id,
         |     a.spread_type as spread_type,
         |     a.pay_price   as pay_price,
         |     a.sub_side_commission as commission_price,
         |     trim(a.sku_uuid)  as  click_id,
         |     b.jda  as jda,
         |     b.PREV_union_id as union_id_start,
         |     b.request_tm_diff as request_tm_diff,
         |     (unix_timestamp(a.order_time) - unix_timestamp(b.request_tm)) as order_request_tm_diff,
         |     if(b.policy_id is null, "", b.policy_id) as policy_id
         | from
         |     (
         |        select *, if(ex is null, "", ex) user_pin
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt = '${date}'
         |     ) a
         |     join
         |     (
         |        select *, unix_timestamp(request_tm) - unix_timestamp(PREV_request_tm) request_tm_diff, if(lcase(trim(user_pin))='null', "", trim(user_pin)) user_pin_
         |        from 
         |            ${database}.cps_fraud_inner_union
         |        where
         |            dt >= '${beginDate}'
         |            and
         |            dt <= '${date}'
         |     ) b
         |     on  a.union_id = b.union_id and a.user_pin = b.user_pin_
         |)t
         |group by 
         |     id, detail_id, order_id, order_time, ad_traffic_type, platform, union_id, sub_union_id, spread_type, pay_price, 
         |     commission_price, click_id, jda, union_id_start, request_tm_diff, order_request_tm_diff, policy_id
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val clickLog = hiveContext.sql(querySql)

    clickLog.rdd.repartition(400).map{ row =>
      var platform = row.getAs[Int]("platform")
      if (null == platform) {
          platform = 1
      }
      Map(
          "id" -> row.getAs[Long]("id"),
          "detail_id" -> getStringOrElse(row, "detail_id"),
          "order_id" -> row.getAs[Long]("order_id"),
          "order_time" -> getStringOrElse(row, "order_time"),
          "ad_traffic_type" -> row.getAs[Int]("ad_traffic_type"),
          "platform" -> platform,
          "union_id" -> row.getAs[Long]("union_id"),
          "sub_union_id" -> getStringOrElse(row, "sub_union_id"),
          "spread_type" -> row.getAs[Int]("spread_type"),
          "pay_price" -> row.getAs[Int]("pay_price"),
          "commission_price" -> row.getAs[Int]("commission_price"),
          "click_id" -> getStringOrElse(row, "click_id"),
          "jda" -> getStringOrElse(row, "jda"),
          "union_id_start" -> row.getAs[Long]("union_id_start"),
          "request_tm_diff" -> row.getAs[Int]("request_tm_diff"),
          "order_request_tm_diff" -> row.getAs[Int]("order_request_tm_diff"),
          "policy_id" -> getStringOrElse(row, "policy_id")
      )
    }.cache
  }
}
