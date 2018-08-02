package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
 *
 * Created by yfchengkai on 2017/9/19.
 */
class UnionAntiOrderDetailLog extends DataExtractor{

  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    val database = argv.outputDB
    val date = new SimpleDateFormat("yyyy-MM-dd").format(argv.day)
    val beginDate = getBeginDateStr(argv.day, 30)
    val batchList = getBatchList(argv.day)
    val batchListStr = getDateListStr(batchList)
    val batchDelList = getBatchDelList(batchList)
    val batchDelListStr = getDateListStr(batchDelList)

    val hiveContext = new HiveContext(sc)

    hiveContext.sql(s"""use ${database}""")

    val querySql: String = s"""
        | select
        |   o.id                                                 as id,
        |   o.detail_id                                          as detail_id,
        |   o.order_id                                           as order_id,
        |   o.order_time                                         as order_time,
        |   o.finish_time                                        as finish_time,
        |   o.valid_flag                                         as valid_flag,
        |   o.comm_flag                                          as comm_flag,
        |   o.order_type                                         as order_type,
        |   o.emt_source                                         as platform,
        |   o.spread_type                                        as spread_type,
        |   case
        |     when o.ref_status> 0 then o.ref_status
        |     when r.policy_id > 200 and r.policy_id < 300 and r.policy_id!=210
        |           and o.ad_traffic_type in (31, 32) then 0
        |     when r.policy_id > 0 then r.policy_id
        |     else 0
        |   end                                                  as cheat_type,
        |   o.union_id                                           as union_id,
        |   o.website_id                                         as sub_union_id,
        |   o.gmv                                                as pay_price,
        |   o.commission                                         as commission_price,
        |   o.click_uuid                                         as anti_click_id,
        |   o.site_id                                            as site_id,
        |   o.ad_traffic_type                                    as ad_traffic_type,
        |   o.jda                                                as jda,
        |   o.d2t                                                as dt
        | from (
        |   select
        |     id                                                 as id,
        |     substring(order_time,1,10)                         as d2t,
        |     order_id                                           as order_id,
        |     detail_id                                          as detail_id,
        |     cast(policy_id as int)                             as policy_id
        |   from ${database}.cps_fraud_union_order_result
        |   where dt in (${batchListStr})
        | ) r right outer join (
        |   select
        |     CAST(id as bigint)                                 as id,
        |     detail_id                                          as detail_id,
        |     order_id                                           as order_id,
        |     order_time                                         as order_time,
        |     finish_time                                        as finish_time,
        |     if(yn=1, 1, 0)                                     as valid_flag,
        |     if(order_state=1 and clear_status=1, 1, 0)         as comm_flag,
        |     spread_type                                        as spread_type,
        |     0                                                  as order_type,
        |     platform                                           as emt_source,
        |     ref_status                                         as ref_status,
        |     union_id                                           as union_id,
        |     if(sub_union_id is null
        |       or trim(sub_union_id)=''
        |       or lcase(trim(sub_union_id))='null',
        |         'null', trim(sub_union_id))                    as website_id,
        |     pay_price                                          as gmv,
        |     coalesce(sub_side_commission, 0)                   as commission,
        |     trim(sku_uuid)                                     as click_uuid,
        |     site_id                                            as site_id,
        |     coalesce(ad_traffic_type, -1)                      as ad_traffic_type,
        |     jda                                                as jda,
        |     substring(order_time, 1, 10)                       as d2t
        |   from antidb.union_commission_allocate_distribution_with_click
        |   where dt>='${beginDate}' and dt<='${date}'
        |         and to_date(order_time) in (${batchDelListStr})
        | ) o on CAST(o.id as bigint)=r.id and o.d2t=r.d2t
        | order by o.order_time
      """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)

    val unionCheatOrderLog = hiveContext.sql(querySql)

    unionCheatOrderLog.rdd.repartition(400).map{ row =>
      Map(
        "id"                  -> row.getAs[Long]("id"),
        "detail_id"           -> row.getAs[Long]("detail_id"),
        "order_id"            -> row.getAs[Long]("order_id"),
        "order_time"          -> row.getAs[String]("order_time"),
        "finish_time"         -> row.getAs[String]("finish_time"),
        "valid_flag"          -> row.getAs[Int]("valid_flag"),
        "comm_flag"           -> row.getAs[Int]("comm_flag"),
        "order_type"          -> row.getAs[Int]("order_type"),
        "platform"            -> row.getAs[Int]("platform"),
        "spread_type"         -> row.getAs[Int]("spread_type"),
        "cheat_type"          -> row.getAs[Int]("cheat_type"),
        "union_id"            -> row.getAs[Long]("union_id"),
        "sub_union_id"        -> row.getAs[String]("sub_union_id"),
        "pay_price"           -> row.getAs[Int]("pay_price"),
        "commission_price"    -> row.getAs[Int]("commission_price"),
        "anti_click_id"       -> row.getAs[String]("anti_click_id"),
        "site_id"             -> row.getAs[Long]("site_id"),
        "ad_traffic_type"     -> row.getAs[Int]("ad_traffic_type"),
        "jda"                 -> row.getAs[String]("jda"),
        "dt"                  -> row.getAs[String]("dt")
      )
    }.cache
  }

  private def getBeginDateStr(date: Date, days: Int): String = {
    val beginDay = Calendar.getInstance()
    beginDay.setTime(date)
    beginDay.add(Calendar.DATE, -days)
    new SimpleDateFormat("yyyy-MM-dd").format(beginDay.getTime)
  }

  private def getBatchList(date: Date): List[String] = {
    var result = List[String]()
    val dayList = List(0, 1, 2, 3, 4, 5, 6, 20, 30)
    for (day <- dayList) {
      val dateRec = Calendar.getInstance()
      dateRec.setTime(date)
      dateRec.add(Calendar.DATE, -day)
      result = result :+ new SimpleDateFormat("yyyy-MM-dd").format(dateRec.getTime)
    }
    result
  }

  private def getBatchDelList(batchList: List[String]): List[String] = {
    var result = List[String]()
    val dayList = List(0, 1, 2, 3, 4, 5, 6, 20)
    for (date <- batchList) {
      for (day <- dayList) {
        val rec = Calendar.getInstance()
        rec.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(date))
        rec.add(Calendar.DATE, -day)
        val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(rec.getTime)
        result = result :+ dateStr
      }
    }
    result.distinct
  }

  private def getDateListStr(batchList: List[String]): String = {
    var result = ""
    for (date <- batchList) {
      result = result + "'" + date + "',"
    }
    result.substring(0, result.length - 1)
  }
}
