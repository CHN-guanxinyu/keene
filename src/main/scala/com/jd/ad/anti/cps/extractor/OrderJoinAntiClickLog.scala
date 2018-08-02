package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import scala.Tuple2

import com.jd.ad.anti.cps.ExecutorArgs

class OrderJoinAntiClickLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    
    //hiveContext.sql("use fdm")
    //2018-06-29:京粉流量不走劫持反作弊(17,142,196)
    var querySql: String = s"""
         | select
         |     a.id id,
         |     a.detail_id detail_id,
         |     a.order_id order_id,
         |     a.order_time order_time,
         |     a.ad_traffic_type ad_traffic_type,
         |     a.platform platform,
         |     a.union_id union_id,
         |     trim(a.sub_union_id) sub_union_id,
         |     a.spread_type spread_type,
         |     a.pay_price pay_price,
         |     a.sub_side_commission as commission_price,
         |     trim(a.sku_uuid) as click_id,
         |     b.click_id as anti_click_id,
         |     b.jda as anti_jda,
         |     if(b.policy_id is null, "", b.policy_id) as policy_id
         | from
         |     (
         |        select *
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt = '${date}'
         |     ) a
         |     left join
         |     (
         |        select *
         |        from 
         |            ${database}.cps_fraud_union_click
         |        where
         |            dt >= '${beginDate}'
         |            and
         |            dt <= '${date}'
         |     ) b
         |     on a.sku_uuid = b.click_id
         |     where a.ad_traffic_type not in (17,142,196)
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val clickLog = hiveContext.sql(querySql)
    val rows = clickLog.rdd.map{ row =>
      Map(
          "id" -> row.getAs[Long]("id"),
          "detail_id" -> getStringOrElse(row, "detail_id"),
          "order_id" -> row.getAs[Long]("order_id"),
          "order_time" -> getStringOrElse(row, "order_time"),
          "ad_traffic_type" -> row.getAs[Int]("ad_traffic_type"),
          "platform" -> row.getAs[Int]("platform"),
          "union_id" -> getStringAsInt(row, "union_id"),
          "sub_union_id" -> getStringOrElse(row, "sub_union_id"),
          "spread_type" -> row.getAs[Int]("spread_type"),
          "pay_price" -> row.getAs[Int]("pay_price"),
          "commission_price" -> row.getAs[Int]("commission_price"),
          "click_id" -> getStringOrElse(row, "click_id"),
          "anti_click_id" -> getStringOrElse(row, "anti_click_id"),
          "anti_jda" -> getStringOrElse(row, "anti_jda"),
          "policy_id" -> getStringOrElse(row, "policy_id")
      )
    }.cache
    val policyCounterMap = rows.map { row =>
      val orderKey = "sub_union_order_num"
      val policyOrderKey = "sub_union_policy_%s_order_num".format(
        row.getOrElse("policy_id", "0").toString)
      val jdaKey = "sub_union_policy_%s_jda_num".format(
        row.getOrElse("policy_id", "0").toString)
      val key = "%d|%s".format(row.getOrElse("union_id", 0),
        row.getOrElse("sub_union_id", ""))
      val jda = row.getOrElse("anti_jda", "").toString
      val jdaSet = if (jda == "") Set[String]() else Set(jda)
      (key, Tuple2(Map(orderKey -> 1, policyOrderKey -> 1), Map(jdaKey -> jdaSet)))
    }.reduceByKey { (a, b) =>
      val orderCounter = (a._1.keys ++ b._1.keys).map { key =>
        Map(key -> (a._1.getOrElse(key, 0) + b._1.getOrElse(key, 0)))
      }.foldLeft(Map[String, Int]())(_++_)
      val jdaCounter = (a._2.keys ++ b._2.keys).map { key => 
        Map(key -> (a._2.getOrElse(key, Set()) ++ b._2.getOrElse(key, Set())))
      }.foldLeft(Map[String, Set[String]]())(_++_)
      Tuple2(orderCounter, jdaCounter)
    }.map { case (key, (orderCounter, jdaCounter)) =>
      (key, orderCounter ++ jdaCounter.map { case (key, values) =>
        (key, values.size)})
    }.collectAsMap
    val bcPolicyCounterMap = sc.broadcast(policyCounterMap)
    rows.map { row =>
        val policyCounterMap = bcPolicyCounterMap.value
        val counterKey = "%d|%s".format(
          row.getOrElse("union_id", 0).asInstanceOf[Int],
          row.getOrElse("sub_union_id", "").asInstanceOf[String])
        row ++ policyCounterMap.getOrElse(counterKey, Map[String, Int]())
    }
  }
}
