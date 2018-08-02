package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date

import com.jd.ad.anti.cps.ExecutorArgs

class OrderJoinInnerOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    
    hiveContext.sql("set hive.groupby.orderby.position.alias = true;")

    var querySql: String = s"""
         | select
         |     a.id as id,
         |     a.detail_id as detail_id,
         |     a.order_id as order_id,
         |     a.order_time as order_time,
         |     a.ad_traffic_type as ad_traffic_type,
         |     a.platform as platform,
         |     a.union_id as union_id,
         |     a.sub_union_id as sub_union_id,
         |     a.spread_type  as spread_type,
         |     a.pay_price as pay_price,
         |     a.commission_price as commission_price,
         |     a.click_id as anti_click_id,
         |     b.union_jda_num  as union_jda_num,
         |     c.sub_union_order_num as sub_union_order_num,
         |     d.sub_union_cheat_num as sub_union_cheat_num,
         |     if(a.policy_id is null, "", a.policy_id) as policy_id
         | from
         |     (  select *
         |        from 
         |            ${database}.cps_fraud_inner_order
         |        where 
         |            dt = '${date}' 
         |     )a
         |     join(
         |        select union_id, count(distinct jda) union_jda_num
         |        from 
         |            ${database}.cps_fraud_inner_order
         |        where
         |            dt = '${date}'
         |        group by
         |            union_id
         |     )b
         |     on a.union_id = b.union_id
         |     join
         |     (
         |        select union_id, if(sub_union_id is null, "", trim(sub_union_id)) sub_union_id, count(id) sub_union_order_num 
         |        from
         |              antidb.union_commission_allocate_distribution_with_click
         |        where
         |              dt = '${date}'
         |        group by 
         |              1, 2
         |     ) c
         |     on  a.union_id = c.union_id and a.sub_union_id = c.sub_union_id
         |     join
         |     (
         |        select union_id, sub_union_id, count(distinct id) sub_union_cheat_num
         |        from
         |            ${database}.cps_fraud_inner_order
         |        where
         |            dt = '${date}'
         |        group by 
         |              union_id, sub_union_id
         |     )d
         |     on a.union_id = d.union_id and a.sub_union_id = d.sub_union_id
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val clickLog = hiveContext.sql(querySql)
    val rows = clickLog.rdd.map{ row =>

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
          "union_jda_num" -> row.getAs[Int]("union_jda_num"),
          "sub_union_order_num" -> row.getAs[Int]("sub_union_order_num"),
          "sub_union_cheat_num" -> row.getAs[Int]("sub_union_cheat_num"),
          "anti_click_id" -> getStringOrElse(row, "anti_click_id"),
          "policy_id" -> getStringOrElse(row, "policy_id")
      )
    }.cache
    val policyCounterMap = rows.map { row =>
      val key = "union_policy_%s_order_num".format(
        row.getOrElse("policy_id", "0").toString)
      (row.getOrElse("union_id", 0), Map(key -> 1))
    }.reduceByKey { (a, b) =>
      (a.keys ++ b.keys).map{ key => 
        Map(key -> (a.getOrElse(key, 0) + b.getOrElse(key, 0))) 
      }.foldLeft(Map[String, Int]())(_++_)
    }.collectAsMap
    val bcPolicyCounterMap = sc.broadcast(policyCounterMap)
    rows.map { row =>
        val policyCounterMap = bcPolicyCounterMap.value
        val unionId = row.getOrElse("union_id", 0).asInstanceOf[Int]
        row ++ policyCounterMap.getOrElse(unionId, Map[String, Int]())
    }
  }
}
