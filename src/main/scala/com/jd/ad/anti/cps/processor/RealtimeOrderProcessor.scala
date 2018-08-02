package com.jd.ad.anti.cps.processor

import com.jd.ad.anti.cps.dumper.RealtimeDataDumper
import com.jd.ad.anti.cps.yaml.{CpsAntiYaml, _Job}
import org.apache.spark.streaming.{StreamingContext, Time}
import com.jd.ad.anti.cps.extractor.RealtimeDataExtractor
import com.jd.ad.anti.cps.filter.{RealtimeFraudClickFilter, RealtimeRuleFilter, RuleFilter}
import com.jd.ad.anti.cps.util.JobIni
import com.jd.ad.anti.cps.realtime.Statistic
import com.jd.ad.anti.cps.variable.RealtimeVariables
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import java.util.HashMap

import com.jd.ad.anti.cps.job.RealtimeJobExecutor.getCount

import scala.collection.mutable.ListBuffer

class RealtimeOrderProcessor extends RealtimeProcessor {
  
  override def process(ssc: StreamingContext,
                       conf: JobIni,
                       statistic: Statistic,
                       extractor: RealtimeDataExtractor,
                       dumper: RealtimeDataDumper): Unit = {
    // Init Rule configuration
    val job: _Job = CpsAntiYaml.getJobByName("OrderJoinClickLogFilterJob")


    if (job == null) {
      println("Missing job name in Yaml file!")
      System.exit(1)
    }

    val realVars = new RealtimeVariables()
    realVars.init(ssc, conf)

    val unionFilterArr: ListBuffer[RuleFilter] = new ListBuffer[RuleFilter]()

    for (rule <- job.filters.toList) {
      if (rule.getFilter_type.equals("RealtimeFraudClickFilter")){
        val fraudClickFilter = new RealtimeFraudClickFilter(rule)
        fraudClickFilter.init(ssc, conf, realVars)
        unionFilterArr.append(fraudClickFilter)
      }else {
        val unionFilter = new RealtimeRuleFilter(rule)
        unionFilter.init(ssc, conf, realVars)
        unionFilterArr.append(unionFilter)
      }
    }

    // 获取订单实时数据
    val orderStream = extractor.getDStream(ssc, conf, statistic)
//    orderStream.print()
    
    val clickStream = orderStream.flatMap{ row =>
      row("clickInfo").asInstanceOf[Array[Map[String,Any]]]
    }

//    clickStream.print()

    // 遍历所有策略
    val unionOrderListStream = clickStream.transform((rdd: RDD[Map[String, Any]], t: Time) => {

      var r:RDD[Map[String, Any]] = null

      for (unionFilter <-  unionFilterArr.toList) {
        val billData = unionFilter.doFilter(rdd)
        if (r == null )
          r = billData
        else
          r = r.union(billData)
      }
      val orderFraudRdd = r.map( row => {
//          statistic.updateTotalRecords(row("policy_id").toString , 1L)
          Map(
            "click_id" -> row("click_id"),
            "order_id" -> row("order_id"),
            "policy_id" -> row("policy_id")
          )
        }
      )
      orderFraudRdd
    })

    // 按订单分组
    val unionFraudOrders = unionOrderListStream.transform(
      (rdd: RDD[Map[String, Any]], t: Time) => {
        rdd.map{ row =>
          (row("order_id"), row)
        }.groupByKey().map { case (key,values) =>
          val tmp = new HashMap[String, Integer]()
          var fraudClickInfo: Array[Map[String,Any]] = Array()
          // 一个clickId只保留policy_id最小的，也就是优先级最高的
          values.foreach( f =>
            if (!tmp.containsKey(f.get("click_id"))
                || tmp.get(f.get("click_id")) >= f.getOrElse("policy_id","").toString.toInt) {
              tmp.put("click_id", f.getOrElse("policy_id","").toString.toInt)
              fraudClickInfo :+= f
            }
          )
          Map (
            "order_id" -> key,
            "fraudClickInfo" -> fraudClickInfo
          )
        }
    })

    // 关联原始订单
    val antiOrder = orderStream.transformWith(unionFraudOrders,
      (orderRdd: RDD[Map[String,Any]], fraudRdd: RDD[Map[String,Any]], t) => {
        val preFraudRdd = fraudRdd.distinct().map { row =>
          (row("order_id"),row)
        }
        val preOrderRdd = orderRdd.map { row =>
          (row("orderId"),row)
        }
        preOrderRdd.leftOuterJoin(preFraudRdd).map{ case(key,values) =>
          if (values._2.nonEmpty){
            values._1 + ("isBill" -> 1) + ("fraudClickInfo" -> values._2.get("fraudClickInfo"))
          }else{
            values._1 + ("isBill" -> 0) + ("fraudClickInfo" -> Array())
          }
        }
      }
    )

//    antiOrder.map(_("orderId")).print()
//    antiOrder.map(_("isBill")).print()

    dumper.saveData(ssc, antiOrder, conf, statistic)

  }
}
