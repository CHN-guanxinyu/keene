package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
 *
 * Created by yfchengkai on 2017/9/6.
 */
class UnionOrderLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {

    val hiveContext = new HiveContext(sc)
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)

    var cal:Calendar = Calendar.getInstance()
    cal = Calendar.getInstance()
    cal.setTime(argv.day)
    cal.add(Calendar.DATE, 1)
    val nextDate = formatter.format(cal.getTime)

    val database = argv.outputDB

    val querySql: String = s"""
         | select
         |    id                                                                   as id,
         |    detail_id                                                            as detail_id,
         |    order_id                                                             as order_id,
         |    order_time                                                           as order_time,
         |    platform                                                             as platform,
         |    union_id                                                             as union_id,
         |    if(sub_union_id is null
         |        or trim(sub_union_id)=''
         |        or lcase(trim(sub_union_id))='null', 'null', trim(sub_union_id)) as sub_union_id,
         |    ad_traffic_type                                                      as ad_traffic_type,
         |    spread_type                                                          as spread_type,
         |    coalesce(pay_price, 0)                                               as pay_price,
         |    coalesce(sub_side_commission, 0)                                     as commission_price,
         |    trim(sku_uuid)                                                       as anti_click_id,
         |    ex                                                                   as user_pin
         | from antidb.union_commission_allocate_distribution_with_click
         | where dt='${date}' and order_time >='${date}' and order_time <'${nextDate}'
      """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)

    val unionOrderLog = hiveContext.sql(querySql)

    unionOrderLog.rdd.repartition(400).map{ row =>
      Map(
        "id"                  -> row.getAs[Long]("id"),
        "detail_id"           -> row.getAs[Long]("detail_id"),
        "order_id"            -> row.getAs[Long]("order_id"),
        "order_time"          -> getStringOrElse(row, "order_time"),
        "ad_traffic_type"     -> row.getAs[Int]("ad_traffic_type"),
        "platform"            -> row.getAs[Int]("platform"),
        "union_id"            -> row.getAs[Long]("union_id"),
        "sub_union_id"        -> row.getAs[String]("sub_union_id"),
        "spread_type"         -> row.getAs[Int]("spread_type"),
        "pay_price"           -> row.getAs[Int]("pay_price"),
        "commission_price"    -> row.getAs[Int]("commission_price"),
        "anti_click_id"       -> getStringOrElse(row, "anti_click_id"),
        "user_pin"            -> getStringOrElse(row, "user_pin"),
        "null_sub_union_id"   -> "null",
        "illegal_sub_union_id"-> "#allthesubsite"
      )
    }.cache
  }
}
