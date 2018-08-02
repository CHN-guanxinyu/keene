package com.jd.ad.anti.cps.extractor

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
/**
 * DNS1作弊策略
 */
class AppAllocateCpsClick1Log  extends DataExtractor{
  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    
    val hiveContext = new HiveContext(sc)
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)
    val formatter1 = new SimpleDateFormat("yyyyMMdd")
    val date1 = formatter1.format(argv.day)
    val beginDate = formatter1.format(argv.beginDay)
    
    hiveContext.sql("use app")

    /**
     * app_cps_click_1_log表与fdm_ads_newunion_click_log join，得到一跳点击
     */
    var querySql: String = s"""
         | select
         |     a.unionid  as union_id, 
         |     b.click_id as click_id, 
         |     a.checkjda as checkjda,
         |     if(a.cunionid is null or trim(a.cunionid)='' or lcase(trim(a.cunionid))='null', 'null', trim(a.cunionid)) as sub_union_id,
         |     a.createtime as click_time,
         |     unix_timestamp(a.createtime) as click_seconds
         | from 
         | (select unionid, checkjda, cunionid, createtime, clickid from
         |       app_cps_click_1_log
         |     where
         |       dt='${date}'
         |       and length(createtime)=19) a
         | left outer join
         | (select clickid click_id, split(ext_columns['csid'],'\\\\^')[0] click_uuid_1_after
         |    from fdm.fdm_ads_newunion_click_log 
         |    where day >= '${beginDate}' and day <='${date1}' and split(ext_columns['csid'],'\\\\^')[0]!='' and ext_columns['csid'] is not null) b
         | on a.clickid=b.click_uuid_1_after
       """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)
    
    val clickLog = hiveContext.sql(querySql)
      
    clickLog.rdd.repartition(200).map{ row =>

      var jda = ""
      var checkjda = getStringOrElse(row, "checkjda", "").split("\\.")
      if (checkjda.length > 1) {
        jda = checkjda(1)
      }
      var union_id = row.getAs[Long]("union_id")
      if (null == union_id) {
        union_id = 0
      }
      var click_seconds = row.getAs[Long]("click_seconds")
      if (null == click_seconds) {
        click_seconds = 0
      }
    
      Map(
        "union_id"        -> union_id,
        "click_id"        -> getStringOrElse(row, "click_id", ""),
        "jda"             -> jda,
        "sub_union_id"    -> getStringOrElse(row, "sub_union_id", ""),
        "click_time"      -> getStringOrElse(row, "click_time", ""),
        "click_seconds"   -> click_seconds
      )
    }.cache
  }
}