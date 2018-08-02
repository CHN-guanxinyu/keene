package com.jd.ad.anti.cps.extractor

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import java.text.SimpleDateFormat
import java.net.URL
import java.net.MalformedURLException  

import com.jd.ad.anti.cps.ExecutorArgs

class UnionClickLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val date = formatter.format(argv.day)
    
    hiveContext.sql("use fdm")

    var querySql: String = s"""
         | select
         |     clickid                                        as click_id,
         |     refer                                          as refer,
         |     tu                                             as target_url,
         |     clicktime                                      as click_time,
         |     unix_timestamp(clicktime)                      as click_seconds,
         |     unionid                                        as union_id,
         |     trim(cuid)                                     as sub_union_id,
         |     case when ext_columns['platform'] = 1
         |          then "pc"
         |          else "mobile"
         |     end                                            as platform,
         |     ext_columns['adSpreadType']                    as spread_type,
         |     ext_columns['adTrafficType']                   as ad_traffic_type,
         |     trim(ip)                                       as click_ip,
         |     trim(ua)                                       as ua,
         |     trim(ext_columns['uuid'])                      as jda,
         |     trim(split(ext_columns['checkjda'], '\\\\.')[2]) as jda_time,
         |     substr(clicktime, 1, 10)                       as dt,
         |     ext_columns['csid']                            as csid,
         |     webtype                                        as webtype
         | from
         |     fdm_ads_newunion_click_log
         | where
         |     day='${date}'
         |     and protype != 12
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val clickLog = hiveContext.sql(querySql)
      
    clickLog.rdd.repartition(200).map{ row =>
      var referHost = ""
      var targetUrlHost = ""
      try {
        referHost = new URL(getStringOrElse(row, "refer", "").toLowerCase).getHost
      } catch {
        case ex: MalformedURLException => Unit
      }
      try {
        targetUrlHost = new URL(getStringOrElse(row, "target_url", "").toLowerCase).getHost
      } catch {
        case ex: MalformedURLException => Unit
      }
      
      Map(
        "click_id"        -> getStringOrElse(row, "click_id", ""),
        "refer"           -> getStringOrElse(row, "refer", ""),
        "target_url"      -> getStringOrElse(row, "target_url", ""),
        "click_time"      -> getStringOrElse(row, "click_time", ""),
        "click_seconds"   -> row.getAs[Long]("click_seconds"),
        "union_id"        -> row.getAs[Long]("union_id"),
        "sub_union_id"    -> getStringOrElse(row, "sub_union_id", ""),
        "platform"        -> getStringOrElse(row, "platform", ""),
        "spread_type"     -> getStringAsInt(row, "spread_type"),
        "ad_traffic_type" -> getStringAsInt(row, "ad_traffic_type"),
        "click_ip"        -> getStringOrElse(row, "click_ip", ""),
        "ua"              -> getStringOrElse(row, "ua", ""),
        "jda"             -> getStringOrElse(row, "jda", ""),
        "jda_time"        -> getStringOrElse(row, "jda_time", ""),
        "dt"              -> getStringOrElse(row, "dt", ""),
        "csid"            -> getStringOrElse(row, "csid", ""),
        "webtype"         -> getStringAsInt(row, "webtype"),
        "refer_host"      -> referHost,
        "target_url_host" -> targetUrlHost
      )
    }.cache
  }
}
