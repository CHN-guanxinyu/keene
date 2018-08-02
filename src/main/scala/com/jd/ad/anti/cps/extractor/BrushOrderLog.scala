package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
 *
 * Created by yfchengkai on 2017/8/30.
 */
class BrushOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {

    val hiveContext = new HiveContext(sc)
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)

    val beginDate = formatter.format(argv.beginDay)

    var cal:Calendar = Calendar.getInstance()
    cal = Calendar.getInstance()
    cal.setTime(argv.day)
    cal.add(Calendar.DATE, 1)
    val nextDate = formatter.format(cal.getTime)

    val database = argv.outputDB

    hiveContext.sql("use app")

    val querySql: String = s"""
         | select
         |    t2.id                                               as id,
         |    t2.detail_id                                        as detail_id,
         |    t2.order_id                                         as order_id,
         |    t2.order_time                                       as order_time,
         |    t2.ad_traffic_type                                  as ad_traffic_type,
         |    t2.platform                                         as platform,
         |    t2.union_id                                         as union_id,
         |    t2.sub_union_id                                     as sub_union_id,
         |    t2.spread_type                                      as spread_type,
         |    t2.pay_price                                        as pay_price,
         |    t2.commission_price                                 as commission_price,
         |    t2.click_uuid                                       as anti_click_id
         | from(
         |    select
         |      substring(sumit_time, 1, 10)                      as dt,
         |      order_id                                          as order_id,
         |      sku_id                                            as sku_id
         |    from app.app_anti_order_fraud_l1_mobile
         |    where dt ='${date}'
         | ) t1 join (
         |    select
         |        id                                              as id,
         |        order_id                                        as order_id,
         |        detail_id                                       as detail_id,
         |        sku_id                                          as sku_id,
         |        order_time                                      as order_time,
         |        platform                                        as platform,
         |        union_id                                        as union_id,
         |        if(sub_union_id is null or trim(sub_union_id)=''
         |          or lcase(trim(sub_union_id))='null',
         |            'null', trim(sub_union_id))                 as sub_union_id,
         |        ad_traffic_type                                 as ad_traffic_type,
         |        spread_type                                     as spread_type,
         |        coalesce(pay_price, 0)                          as pay_price,
         |        coalesce(sub_side_commission, 0)                as commission_price,
         |        trim(sku_uuid)                                  as click_uuid,
         |        substring(order_time, 1, 10)                    as dt
         |    from antidb.union_commission_allocate_distribution_with_click
         |    where dt>='${beginDate}' and order_time >= '${beginDate}' and order_time < '${nextDate}'
         | ) t2 on (t1.dt=t2.dt and t1.order_id=t2.order_id)
         | group by t2.id, t2.order_id, t2.detail_id, t2.order_time, t2.ad_traffic_type, t2.platform, t2.union_id,
         |          t2.sub_union_id, t2.spread_type, t2.pay_price, t2.commission_price, t2.click_uuid
      """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)

    val brushOrderLog = hiveContext.sql(querySql)

    brushOrderLog.rdd.repartition(400).map{ row =>
      Map(
        "id"               -> row.getAs[Long]("id"),
        "detail_id"        -> row.getAs[Long]("detail_id"),
        "order_id"         -> row.getAs[Long]("order_id"),
        "order_time"       -> getStringOrElse(row, "order_time"),
        "ad_traffic_type"  -> row.getAs[Int]("ad_traffic_type"),
        "platform"         -> row.getAs[Int]("platform"),
        "union_id"         -> row.getAs[Long]("union_id"),
        "sub_union_id"     -> getStringOrElse(row, "sub_union_id"),
        "spread_type"      -> row.getAs[Int]("spread_type"),
        "pay_price"        -> row.getAs[Int]("pay_price"),
        "commission_price" -> row.getAs[Int]("commission_price"),
        "anti_click_id"    -> getStringOrElse(row, "anti_click_id")
      )
    }.cache
  }
}
