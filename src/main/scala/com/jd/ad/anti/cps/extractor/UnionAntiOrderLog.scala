package com.jd.ad.anti.cps.extractor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 *
 * Created by yfchengkai on 2017/9/19.
 */
class UnionAntiOrderLog extends DataExtractor{

  override def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]] = {
    val hiveContext = new HiveContext(sc)

    val policyIds = argv.exportPolicyIds 
    val database = argv.outputDB

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(argv.day)
    val beginDate = formatter.format(argv.beginDay)
    val nextDate = formatter.format(argv.nextDay)

    //完成时间,finish_time 默认值为空串(finish_time + 26 > today 未结佣)
    //已过结佣期口径: finish_time不为空 && finish_time + 26 <= today
    val cal:Calendar = Calendar.getInstance()
    val today = formatter.format(cal.getTime)
    cal.add(Calendar.DATE, -26)
    val finishDate = formatter.format(cal.getTime)

    val querySql: String = s"""
        |-- 提取作弊订单,与10类、300类去重,被之前result判过的去重
        | select
        |   l.id                                   as id,
        |   detail_id                              as detail_id,
        |   order_id                               as order_id,
        |   order_time                             as order_time,
        |   ad_traffic_type                        as ad_traffic_type,
        |   platform                               as platform,
        |   union_id                               as union_id,
        |   sub_union_id                           as sub_union_id,
        |   spread_type                            as spread_type,
        |   pay_price                              as pay_price,
        |   commission_price                       as commission_price,
        |   anti_click_id                          as anti_click_id,
        |   policy_id                              as policy_id
        | from
        | (
        |       select * 
        |       from 
        |             ${database}.cps_fraud_union_order
        |       where
        |             dt='${date}' and policy_id in (${policyIds})
        | )l
        | left join
        | (
        | -- 已被其他离线策略判过作弊
        |       select id
        |       from
        |             ${database}.cps_fraud_union_order_result
        |       where
        |             dt>='${beginDate}' and dt<= '${date}'
        |       
        |       union all
        |
        | --  被实时系统判过作弊 or 已过结佣期
        |       select CAST(id as bigint)
        |       from 
        |             fdm.fdm_union_commission_allocate_0_distribution_chain
        |       where 
        |             dp='ACTIVE' and order_time >= '${beginDate}' and order_time <'${nextDate}' 
        |             and (ref_status != 0 or ( length(finish_time) > 0 and to_date(finish_time) <= '${finishDate}') )
        | )r
        | on l.id = r.id
        | where r.id is null
        | group by l.id, detail_id, order_id, order_time, ad_traffic_type, platform, union_id,
                   sub_union_id, spread_type, pay_price, commission_price, anti_click_id, policy_id
      """.stripMargin

    println("Start extractor with query sql:")
    println(querySql)

    val antiOrderLog = hiveContext.sql(querySql)

    val data = antiOrderLog.rdd.repartition(400).map{ row =>
      Map(
        "id"               -> row.getAs[Long]("id"),
        "detail_id"        -> getStringOrElse(row, "detail_id"),
        "order_id"         -> row.getAs[Long]("order_id"),
        "order_time"       -> getStringOrElse(row, "order_time"),
        "ad_traffic_type"  -> row.getAs[Int]("ad_traffic_type"),
        "platform"         -> row.getAs[Int]("platform"),
        "union_id"         -> row.getAs[Long]("union_id"),
        "sub_union_id"     -> getStringOrElse(row, "sub_union_id"),
        "spread_type"      -> row.getAs[Int]("spread_type"),
        "pay_price"        -> row.getAs[Int]("pay_price"),
        "commission_price" -> row.getAs[Int]("commission_price"),
        "anti_click_id"    -> getStringOrElse(row, "anti_click_id"),
        "policy_id"        -> row.getAs[String]("policy_id")
      )
    }.cache

// 被多个policy_id命中的order-id,按照优先级去重,只保留一份
    data.map { row =>
      val key = row.getOrElse("id", 0L)
      (key, row)
    }.groupByKey.flatMap { case(k, v) =>
      val rows:List[Map[String, Any]] = v.toList
      getHighPriorityRow(rows, policyIds)
    }.distinct.cache
  }

//提取作弊订单,按照policyIds优先级去重,在前者优先级高 
  private def getHighPriorityRow(rows: List[Map[String, Any]], policyIds: String): ListBuffer[Map[String, Any]] = {
    //val policyList = List("200","201","202","203","205","206","207","209","211","212")
    val policyList = policyIds.split(",").map(_.trim).toList
    val result:ListBuffer[Map[String, Any]] = new ListBuffer[Map[String, Any]]()
    val loop = new Breaks()
    var matched = false
    loop.breakable {
      for (policy_id <- policyList) {
        for (row <- rows) {
          if (row.getOrElse("policy_id", "").equals(policy_id)) {
            result.append(row)
            matched = true
          }
        }
        if(matched)loop.break()
      }
    }
    result
  }
}
