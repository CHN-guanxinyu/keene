package com.jd.ad.anti.cps.extractor

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import java.text.SimpleDateFormat

import com.jd.ad.anti.cps.ExecutorArgs

class GdmOnlineLog extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    val hiveContext = new HiveContext(sc)
    val formatter = new SimpleDateFormat("yyy-MM-dd")
    val date = formatter.format(argv.day)

    hiveContext.sql("use gdm")

    var querySql: String = s"""
      | select
      |     browser_uniq_id   as  jda,  
      |     user_log_acct     as  user_pin,
      |     request_tm        as  request_tm,
      |     referer_url       as  refer,
      |-- visit_times,sequence_num is null会导致字段值异常,影响sort。若为null,设为0
      |     if(visit_times is null, 0, visit_times) as visit_times,
      |     if(sequence_num is null, 0, sequence_num) as sequence_num,
      |     if(ct_utm_union_id is null, 0, ct_utm_union_id) as union_id
      | from
      |     gdm_online_log
      | where
      | dt='${date}' and utm_campaign not in ('tuiguang', 'cpc', 'none', 'referral', 'organic') and length(browser_uniq_id) > 3
    """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)

    val gdmLog = hiveContext.sql(querySql)

    gdmLog.rdd.repartition(400).map{ row =>

      Map(
        "jda"         -> getStringOrElse(row, "jda", ""),
        "user_pin"    -> getStringOrElse(row, "user_pin", ""),
        "union_id"    -> row.getAs[Long]("union_id"),
        "request_tm"  -> getStringOrElse(row, "request_tm", ""),
        "refer"       -> getStringOrElse(row, "refer", ""),
        "visit_times" -> row.getAs[Int]("visit_times"),
        "sequence_num"-> row.getAs[Int]("sequence_num")
      )
    }.cache
  }
}
