package com.jd.ads_anti.sync

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.core.parsers.{Arguments, ArgumentsParser => Parser}
import org.apache.spark.sql.DataFrame

class SearchAppPv extends Runner {
  override def run (args: Array[ String ]): Unit = {
    val arg = Parser[Args](args)
    implicit val date : String = arg.date

    //input
    Map(
      "log_mark"    ->  fetchGdmOnlineLogMark,
      "online_log"  ->  fetchGdmM14WirelessOnlineLog
    ).foreach{ case (table , df) => df.createOrReplaceTempView( table )}

    """
      select
        log_mark.*,
        if( online_log.browser_uniq_id is null, 0, 1) as has_behavior
      from
        log_mark
        left join online_log
        on log_mark.browser_uniq_id = online_log.browser_uniq_id
    """.go.createOrReplaceTempView("result")

    s"""
      insert overwrite table
        ${arg.resultTable}
      partition (dt = '$date')
      select * from result
    """.go
  }

  def fetchGdmOnlineLogMark(implicit date : String) : DataFrame =
    s"""
      select
        error_cd,error_desc,error_original_data,request_tm,user_visit_ip,ct_url,session_id,web_site_id,use_java_flag,screen_colour_depth,screen_resolution,browser_code_mode,browser_lang_type,page_title,url_domain,flash_ver,os,browser_type,browser_ver,first_session_create_tm,prev_visit_create_tm,visit_create_tm,visit_times,sequence_num,utm_source,utm_campaign,utm_medium,utm_term,log_type1,log_type2,browser_uniq_id,user_log_acct,referer_url,request_par,referer_par,dry_url,item_first_cate_id,item_second_cate_id,item_third_cate_id,sku_id,sale_ord_id,referer_dry_url,referer_item_first_cate_id,referer_item_second_cate_id,referer_item_third_cate_id,referer_sku_id,referer_ord_id,referer_kwd,referer_page_num,request_time_sec,user_visit_ip_id,ord_content,marked_skus,jshop_app_case_id,item_qtty,real_view_flag,ext_columns,load_sec,kwd,page_num,from_position,from_content_id,impressions,first_request_flag,last_request_flag,session_rt,stm_rt,referer_sequence_num,mtm_rt,url_request_seq_num,src_url,src_dry_url,src_kwd,src_page_num,landing_url,stm_max_pv_qtty,mtm_max_pv_qtty,later_orders,from_position_seq_num,from_sys,parsed_url_par,ct_url_anchor,parsed_referer_par,shop_id,referer_shop_id,ct_utm_union_id,ct_utm_sub_union_id,ct_utm_src,ct_utm_medium,ct_utm_compaign,ct_utm_term,from_content_type,from_ad_par,list_page_item_filter,id_list,referer_id_list,src_first_domain,src_second_domain,common_list,chan_first_cate_cd,chan_second_cate_cd,chan_third_cate_cd,chan_fourth_cate_cd,pinid,page_extention,user_site_addr,user_site_cy_name,user_site_province_name,user_site_city_name,all_domain_uniq_id,all_domain_visit_times,all_domain_session_id,all_domain_sequence_num,cdt_flag
      from gdm.gdm_online_log_mark
      where dt='$date'
      and log_type1 = 'mapp.000001'
      and ext_columns['client'] != 'm'
    """ go

  def fetchGdmM14WirelessOnlineLog(implicit date : String) : DataFrame=
    s"""
      select
        distinct browser_uniq_id
      from gdm.gdm_m14_wireless_online_log
      where dt = '$date'
      and length(browser_uniq_id) > 0
    """ go
}

class Args(
  var date : String = "",
  var resultTable : String = ""
) extends Arguments {
  override def usage =
    """
      |Options:
      |
      |--date
      |--result-table     结果表
    """.stripMargin
}
