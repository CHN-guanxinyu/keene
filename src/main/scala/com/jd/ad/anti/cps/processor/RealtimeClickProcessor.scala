package com.jd.ad.anti.cps.processor

import com.jd.ad.anti.cps.dumper.RealtimeDataDumper
import com.jd.ad.anti.cps.extractor.{RealtimeDataExtractor, RealtimeUnionClickKafka}
import com.jd.ad.anti.cps.filter.{RealtimeDnsFilter, RealtimeRuleFilter}
import com.jd.ad.anti.cps.job.RealtimeJobExecutor.getCount
import com.jd.ad.anti.cps.realtime.{CheckPoint, CheckPointHolder, Statistic, JobMonitor}
import com.jd.ad.anti.cps.util.JobIni
import com.jd.ad.anti.cps.variable.RealtimeVariables
import com.jd.ad.anti.cps.yaml.{CpsAntiYaml, _Job}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

import scala.collection.JavaConversions._

/**
  * Created by haojun on 2017/9/13.
  */
class RealtimeClickProcessor extends RealtimeProcessor {
  override def process (
    ssc: StreamingContext,
    conf: JobIni,
    statistic: Statistic,
    extractor: RealtimeDataExtractor,
    dumper: RealtimeDataDumper
  ): Unit = {

    // Init Rule configuration
    val job: _Job = CpsAntiYaml.getJobByName("RealtimeClickLogFilterJob")

    if (job == null) {
      println("Missing job name in Yaml file!")
      System.exit(1)
    }

    val realVars = new RealtimeVariables()
    realVars.init(ssc, conf)

    //    var httpFilter:RealtimeRuleFilter = null
    var dnsFilter: RealtimeDnsFilter = null

    for (rule <- job.filters.toList) {
      //      if (rule.policy_id == 300) {
      //        httpFilter = new RealtimeRuleFilter(rule)
      //        httpFilter.init(ssc, conf, realVars)
      //      }
      if (rule.policy_id == 301) {
        dnsFilter = new RealtimeDnsFilter(rule)
        dnsFilter.init(ssc, conf, realVars)
      }
    }

    // 获取实时点击流数据
    val clkStream = extractor.getDStream(ssc, conf, statistic) // Run Http Rule
    //    val httpFraudStream = clkStream.transform((rdd: RDD[Map[String, Any]]) => {
    //      statistic.startJob("300")
    //      val r = httpFilter.doFilter(rdd)
    //      statistic.endJob("300")
    //
    //      r
    //    })
    //    httpFraudStream.print(2)
    // Run Dns Rule
    val dnsFraudStream = clkStream.window(Duration(conf.getWindowDuration()), Duration(conf.getWindowSlideDuration())).transform((rdd: RDD[ Map[ String, Any ] ]) => {
      statistic.startJob("click_301")
      val r = dnsFilter.doFilter(rdd)
      statistic.endJob("click_301")

      val fraudRdd = r.map { row =>
        Map("click_id" -> row("click_id"), "policy_id" -> row("policy_id"), "click_seconds" -> row("click_seconds"), "union_id" -> row("union_id"), "refer" -> row("refer"), "ad_traffic_type" -> row("ad_traffic_type"), "platform" -> row("platform"))
      }.distinct()

      fraudRdd
    })

    //    dnsFraudStream.print(2)
    //    val allFraudClickStream = httpFraudStream.transformWith(dnsFraudStream,
    //      (httpRdd: RDD[Map[String,Any]], dnsRdd:  RDD[Map[String,Any]],t) => {
    //
    //        val fraudRdd = httpRdd.union(dnsRdd).map { row =>
    //          Map(
    //            "click_id" ->  row("click_id"),
    //            "policy_id" -> row("policy_id"),
    //            "click_seconds" -> row("click_seconds"),
    //            "union_id" -> row("union_id"),
    //            "refer" -> row("refer"),
    //            "ad_traffic_type" -> row("ad_traffic_type"),
    //            "platform" -> row("platform")
    //          )
    //        }.distinct()
    //
    //        fraudRdd
    //      }
    //    )
    //    allFraudClickStream.print()
    dumper.saveData(ssc, dnsFraudStream, conf, statistic)
  }
}
