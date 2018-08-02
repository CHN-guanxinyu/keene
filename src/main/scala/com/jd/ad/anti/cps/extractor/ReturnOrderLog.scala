package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date

import com.jd.ad.anti.cps.ExecutorArgs

class ReturnOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    
    var cal:Calendar = Calendar.getInstance()
    cal.setTime(argv.day)
    cal.add(Calendar.DATE, -16)
    val finishDate = formatter.format(cal.getTime)
    
    hiveContext.sql("set hive.groupby.orderby.position.alias = true;")
    //获取订单完成时间在T-16天的未被判为作弊的cps订单的物流信息
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
         |     c.processinfo as processinfo
         | from
         |     (
         |        select  *
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            dt >= '${beginDate}' and dt<='${finishDate}' and yn=1 and ref_status=0 and clear_status=1 and sku_clear_status = 1
         |     )b
         |     join(
         |        select sale_ord_id order_id, trim(lower(carry_bill_id)) carry_id
         |        from gdm.gdm_m04_ord_det_sum
         |        where dt>= '${beginDate}' and to_date(ord_complete_tm)='${finishDate}' and carry_bill_id is not null and instr(lower(carry_bill_id),'v')=0 and length(carry_bill_id)>10 
         |     )a
         |     on b.order_id = a.order_id
         |    join(
         |       select trim(lower(jobno)) carry_id, processinfo
         |       from fdm.fdm_ps3pl_jobnorouting_jobnotracedetail_chain
         |       where dp='ACTIVE' and to_date(processdate)>='${beginDate}' and jobno is not null
         |       group by 1, 2
         |    )c
         |    on a.carry_id = c.carry_id
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
          "processinfo" -> getStringOrElse(row, "processinfo"),
          "anti_click_id" -> getStringOrElse(row, "anti_click_id")
      ) 
    }

    rows.map { row =>
      val value = row.getOrElse("processinfo", 0).toString
      val key = row - "processinfo"
      (key, value)
    }.groupByKey.map{ case(rows, info) =>
      val carryInfo = info.toList
      var newRows = rows + ("carryInfo" -> carryInfo)
      newRows
    }.distinct.cache
  }
}
