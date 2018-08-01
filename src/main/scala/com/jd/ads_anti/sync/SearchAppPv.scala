package com.jd.ads_anti.sync

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.core.parsers.{Arguments, ArgumentsParser => Parser}
import com.keene.spark.utils.SimpleSpark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

class SearchAppPv extends Runner with SimpleSpark{
  override def run (args: Array[ String ]): Unit = {
    val arg = Parser[Args](args)
    implicit val date : String = arg.date

    //input
    val tables = "log_mark" :: "online_log" :: Nil
    val dataframes @ List(logMark, onlineLog) =
      fetchGdmOnlineLogMark.cache :: fetchGdmM14WirelessOnlineLog.cache :: Nil

    val dfMapper = tables.zip( dataframes )

    dfMapper.foreach{ case (table , df) =>
      df.createOrReplaceTempView( table ) }

    val distinctedJoinKLogMark = "select distinct browser_uniq_id from log_mark".go

    val logMarkExceptOnlineLog = logMark.except( distinctedJoinKLogMark )



    def broadcastMap(df : DataFrame) =
      sc.broadcast( df.as[String].map(_ -> 0).collect.toMap ).value


    val hasBehavior =
      if( logMarkExceptOnlineLog.count < distinctedJoinKLogMark.count / 2 ){
        val exceptBc = broadcastMap(logMarkExceptOnlineLog)
        k: String => if( exceptBc.contains(k) ) 0 else 1
      }
      else{
        val onlineLogBc = broadcastMap(onlineLog)
        k: String => if( onlineLogBc.contains(k) ) 1 else 0
      }

    spark.udf.register("hasBehavior", hasBehavior )


      //result view
    """
      select
        log_mark.*,
        hasBehavior(browser_uniq_id) as has_behavior
      from
        log_mark
    """.go.
      write.
      mode("overwrite").
      orc(arg.tempPath)

    s"load data inpath '${arg.tempPath}' overwrite into table ${arg.resultTable} partition (dt='$date')" go

  }

  def fetchGdmOnlineLogMark(implicit date : String) : DataFrame =
    s"""
      select
        error_cd,error_desc,error_original_data,request_tm,user_visit_ip,ct_url,session_id,web_site_id,use_java_flag,screen_colour_depth,screen_resolution,browser_code_mode,browser_lang_type,page_title,url_domain,flash_ver,os,browser_type,browser_ver,first_session_create_tm,prev_visit_create_tm,visit_create_tm,visit_times,sequence_num,utm_source,utm_campaign,utm_medium,utm_term,log_type1,log_type2,browser_uniq_id,user_log_acct,referer_url,request_par,referer_par,dry_url,item_first_cate_id,item_second_cate_id,item_third_cate_id,sku_id,sale_ord_id,referer_dry_url,referer_item_first_cate_id,referer_item_second_cate_id,referer_item_third_cate_id,referer_sku_id,referer_ord_id,referer_kwd,referer_page_num,request_time_sec,user_visit_ip_id,ord_content,marked_skus,jshop_app_case_id,item_qtty,real_view_flag,ext_columns,load_sec,kwd,page_num,from_position,from_content_id,impressions,first_request_flag,last_request_flag,session_rt,stm_rt,referer_sequence_num,mtm_rt,url_request_seq_num,src_url,src_dry_url,src_kwd,src_page_num,landing_url,stm_max_pv_qtty,mtm_max_pv_qtty,later_orders,from_position_seq_num,from_sys,parsed_url_par,ct_url_anchor,parsed_referer_par,shop_id,referer_shop_id,ct_utm_union_id,ct_utm_sub_union_id,ct_utm_src,ct_utm_medium,ct_utm_compaign,ct_utm_term,from_content_type,from_ad_par,list_page_item_filter,id_list,referer_id_list,src_first_domain,src_second_domain,common_list,chan_first_cate_cd,chan_second_cate_cd,chan_third_cate_cd,chan_fourth_cate_cd,pinid,page_extention,user_site_addr,user_site_cy_name,user_site_province_name,user_site_city_name,all_domain_uniq_id,all_domain_visit_times,all_domain_session_id,all_domain_sequence_num,cdt_flag
      from gdm.gdm_online_log_mark
      where dt='$date'
      and log_type1 = 'mapp.000001'
      and ext_columns['client'] != 'm'
    """.go

  def fetchGdmM14WirelessOnlineLog(implicit date : String) : DataFrame=
    s"""
      select
        distinct browser_uniq_id
      from gdm.gdm_m14_wireless_online_log
      where dt = '$date'
      and length(browser_uniq_id) > 0
    """.go

  def joinDataHasBehavior(a : DataFrame, b : DataFrame): Unit ={
    val except = sc.broadcast( b.as[String].map(_ -> 0).collect.toMap ).value
    a.withColumn("has_behavior", lit(0) ).mapPartitions(_.map{ case record : Row =>
      record.
    })
  }

  def joinDataNonBehavior(a: DataFrame, b: DataFrame): Unit ={
    val except = sc.broadcast( b.as[String].map(_ -> 0).collect.toMap ).value
  }
}

class Args(
  var numRepartition : Int = 2000,
  var date : String = "",
  var resultTable : String = "",
  var tempPath : String = ""
) extends Arguments {
  override def usage =
    """
      |Options:
      |
      |--date
      |--num-repartition
      |--temp-path        写结果表前首先存储到hdfs的路径
      |--result-table     结果表
    """.stripMargin
}
