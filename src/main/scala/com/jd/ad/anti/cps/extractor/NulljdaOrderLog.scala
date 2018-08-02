package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import scala.Tuple2

import com.jd.ad.anti.cps.ExecutorArgs

class NullJdaOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val database = argv.outputDB
    val formatter1 = new SimpleDateFormat("yyyy-MM-dd")
    val formatter2 = new SimpleDateFormat("yyyyMMdd")
    val date = formatter1.format(argv.day)
    val day = formatter2.format(argv.day)
    val beginDay = formatter2.format(argv.beginDay)
    

    var querySql: String = s"""
         | -- 当前只针对子联盟的ad_traffic_type=6的订单进行过滤,限定ad_traffic_type=6
         | -- sub_union_id做相关处理,只取_前的部分
         | -- 2018-04-19：引入1号店ad_traffic_type=172,当前只针对2个流量，where 避免无用数据进入filter
         | -- 增加特征:点击-下单时间差
         | select
         |     b.id as id,
         |     b.detail_id as detail_id,
         |     b.order_id as order_id,
         |     b.order_time as order_time,
         |     b.ad_traffic_type as ad_traffic_type,
         |    -- 订单只分为pc,mobile 这2种情况
         |     case when platform = 1
         |          then "pc"
         |          else "mobile"
         |     end                    as device_type,
         |     b.platform as platform,
         |     b.union_id as union_id,
         |     b.sub_union_id as sub_union_id,
         |     b.spread_type  as spread_type,
         |     b.pay_price as pay_price,
         |     b.sub_side_commission as commission_price,
         |     trim(b.sku_uuid) as anti_click_id,
         |     b.order_seconds as order_seconds,
         |     a.click_seconds as click_seconds,
         |     a.anti_jda as anti_jda
         | from
         |     (
         |        select id, detail_id, order_id, order_time, ad_traffic_type, platform, union_id, spread_type, pay_price, sub_side_commission, sku_uuid,
         |        split(sub_union_id,'_')[0] as sub_union_id,
         |        unix_timestamp(order_time) as order_seconds
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt = '${date}' and ad_traffic_type in (6,172)
         |     )b
         |     join(
         |        select clickid, 
         |        trim(ext_columns['uuid']) as anti_jda,
         |        unix_timestamp(clicktime) as click_seconds
         |        from
         |            fdm.fdm_ads_newunion_click_log
         |        where 
         |            day>=${beginDay} and day<=${day} 
         |--click_seconds 有为空情况，排除 
         |            and unix_timestamp(clicktime) is not null
         |     )a
         |     on a.clickid = b.sku_uuid
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
          "device_type" -> getStringOrElse(row, "device_type"),
          "platform" -> row.getAs[Int]("platform"),
          "union_id" -> row.getAs[Long]("union_id"),
          "sub_union_id" -> getStringOrElse(row, "sub_union_id"),
          "spread_type" -> row.getAs[Int]("spread_type"),
          "pay_price" -> row.getAs[Int]("pay_price"),
          "commission_price" -> row.getAs[Int]("commission_price"),
          "anti_click_id" -> getStringOrElse(row, "anti_click_id"),
          "anti_jda" -> getStringOrElse(row, "anti_jda"),
          "null_sub_union_id"   -> "null",
          "order_seconds" -> row.getAs[Long]("order_seconds"),
          "click_seconds" -> row.getAs[Long]("click_seconds")
      )
    }.cache
    val subUnionCounterMap = rows.map { row =>
    // 计算子联盟订单数,区分子联盟在不同adt上的订单数
      val key = "%s|%s|%s|%d".format(
        row.getOrElse("union_id", 0).toString,
        row.getOrElse("sub_union_id", ""),
        row.getOrElse("device_type",""),
        row.getOrElse("ad_traffic_type", 0))
      val orderId = row.getOrElse("order_id", 0L).toString
      val jda = row.getOrElse("anti_jda", "").toString
      //子联盟日订单数, jda为空的订单数
      val orderSet = Set(orderId)
      val nullJdaOrderSet = if (jda == "") Set(orderId) else Set[String]()
      (key, Tuple2(orderSet, nullJdaOrderSet))
    }.reduceByKey { (a, b) =>
      val orderCounter = a._1 ++ b._1 
      val nullJdaOrderCounter = a._2 ++ b._2
      Tuple2(orderCounter, nullJdaOrderCounter)
    }.map{case(key, (orderCounter, nullJdaOrderCounter)) =>
      val orderKey = "sub_union_order_num"
      val jdaKey = "sub_union_null_jda_order_num"
      (key, Map(orderKey -> orderCounter.size, jdaKey -> nullJdaOrderCounter.size))
    }.collectAsMap
    val bcSubUnionCounterMap = sc.broadcast(subUnionCounterMap)
    rows.map { row =>
        val subUnionCounterMap = bcSubUnionCounterMap.value
        val key = "%s|%s|%s|%d".format(
          row.getOrElse("union_id", 0).toString,
          row.getOrElse("sub_union_id", ""),
          row.getOrElse("device_type",""),
          row.getOrElse("ad_traffic_type",0))
        row ++ subUnionCounterMap.getOrElse(key, Map[String, Int]())
    }
  }
}
