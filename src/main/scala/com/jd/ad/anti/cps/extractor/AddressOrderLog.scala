package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.jd.ad.anti.cps.ExecutorArgs

class AddressOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val database = argv.outputDB
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(argv.day)
    cal.add(Calendar.DATE, -16)
    // 取 T-17 天前的订单数据
    val finishDate = formatter.format(cal.getTime)
    
    hiveContext.sql("set hive.groupby.orderby.position.alias = true;")
    //获取订单完成时间在T-16天的未被判为作弊的cps订单的物流信息
    val querySql: String = s"""
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
         |     a.carry_id as carry_id
         | from
         |     (
         |        select  id,
         |                detail_id,
         |                order_id,
         |                order_time,
         |                ad_traffic_type,
         |                platform,
         |                union_id,
         |                sub_union_id,
         |                spread_type,
         |                pay_price,
         |                sub_side_commission,
         |                sku_uuid
         |        from 
         |            antidb.union_commission_allocate_distribution_with_click
         |        where
         |            -- ref_status=0 不取已判作弊的订单，clear_status=1 不取状态为被清洗的订单（清洗掉不予结佣）
         |            dt >= '${beginDate}' and dt <= '${date}' and yn=1 and ref_status=0 and clear_status=1 and sku_clear_status = 1
         |     )b
         |     join(
         |        select sale_ord_id order_id, trim(lower(carry_bill_id)) carry_id
         |        from gdm.gdm_m04_ord_det_sum
         |        where dt>= '${beginDate}' and to_date(ord_complete_tm)='${finishDate}'
         |          -- 限制运单号，不取京东物流的（包含'v'的是京东物流），不取无效的，运单号正常应大于10个字符
         |          and carry_bill_id is not null and instr(lower(carry_bill_id),'v')=0 and length(carry_bill_id)>10
         |     )a
         |     on b.order_id = a.order_id
         |group by 
         |        1,2,3,4,5,6,7,8,9,10,11,12,13
       """.stripMargin

    val processSql: String =
      s"""
         |select trim(lower(jobno)) as carry_id,
         |       processdate as process_date,
         |       processinfo as process_info
         |  -- 物流信息表
         |  from fdm.fdm_ps3pl_jobnorouting_jobnotracedetail_chain
         |  -- 取50天数据
         | where dp='ACTIVE' and to_date(processdate)>='${beginDate}' and to_date(processdate)<='${date}'
         |       -- 运单号正常应大于10个字符
         |       and jobno is not null and  instr(lower(jobno),'v')=0 and length(jobno) > 10
         | group by 1, 2, 3
       """.stripMargin

    val processRdd = hiveContext.sql(processSql).rdd.map{ row =>
      val key = getStringOrElse(row, "carry_id")
      val processDate = getStringOrElse(row, "process_date")
      val processInfo = getStringOrElse(row, "process_info")
      val value = Tuple2(processDate, processInfo)
      (key, value)
    }
    val processInfo = processRdd.groupByKey.filter(_._2.size > 15) // 物流信息应大于15条
     .map{ case(key,values) =>
        val infoList = values.toList.sortBy(_._1).map(_._2)
        (key, infoList)
    }
    
    val orderLog = hiveContext.sql(querySql)

    val orderRdd = orderLog.rdd.map { row =>
      val key = getStringOrElse(row, "carry_id")
      val value = Map(
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
        "anti_click_id" -> getStringOrElse(row, "anti_click_id")
      )
      (key, value)
    }

    orderRdd.join(processInfo).map { case (key, (orderInfo, addressInfo)) =>
      orderInfo + ("carryInfo" -> addressInfo.mkString("||"))
    }
  }
}
