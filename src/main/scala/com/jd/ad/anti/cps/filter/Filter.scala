package com.jd.ad.anti.cps.filter

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.jd.ad.anti.cps.yaml._
import com.jd.ad.anti.cps.variable.AbstractVariable
import scala.collection.JavaConversions._
import scala.{Tuple2, Tuple3}

abstract class Filter(conf: _Filter) {
  var vars: Map[String, Any] = Map()
  var rules: List[Tuple3[Space, Tuple3[String, String, Map[String, Int]], Space]] = List()
  val policyId: Int = conf.policy_id

  def init(sc: SparkContext,argv: ExecutorArgs): Unit = {
    for ( (k, v) <- conf.variables.toMap ) {
      if (k.toUpperCase == k) {
        val obj = Class.forName(v).newInstance.asInstanceOf[AbstractVariable]
        obj.init(sc,argv)
        vars += k -> obj
      } else {
        vars += k -> v
      }
    }

    initRules()
  }

  def initRules(): Unit = {
    for ( rule <- conf.rules.toList ) {
      println("space: " + rule.space)
      println("filter_space: " + rule.filter_space)
      println("group_key: " + rule.group.group_key)
      println("sort_key: " + rule.group.sort_key)
      val clickTimeDiffRange : Map[String,Int] = Map(
        "lower_seconds" -> rule.click_time_diff_range.lower_seconds,
        "upper_seconds" -> rule.click_time_diff_range.upper_seconds
      )
      println("click_time_diff_range: " + clickTimeDiffRange)
      val r = (new Space(rule.space, vars),
        (rule.group.group_key, rule.group.sort_key, clickTimeDiffRange),
        new Space(rule.filter_space, vars))
      rules = r :: rules
    }
  }

  def doFilter(data: RDD[Map[String, Any]]): RDD[Map[String, Any]]

  def getStringOrElse(record: Map[String, Any], key: String, value: String): String = {
    record.getOrElse(key, value).asInstanceOf[String]
  }

  def getLongOrElse(record: Map[String, Any], key: String, value: Long): Long = {
    record.getOrElse(key, value).asInstanceOf[Long]
  }
}
