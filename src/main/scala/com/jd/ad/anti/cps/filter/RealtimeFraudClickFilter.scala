package com.jd.ad.anti.cps.filter

import com.jd.ad.anti.cps.entity.CpsCheatClick
import com.jd.ad.anti.cps.util.{JimdbClient, JobIni}
import com.jd.ad.anti.cps.variable.RealtimeVariables
import com.jd.ad.anti.cps.yaml._Filter
import com.jd.jim.cli.Cluster
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

/**
  * Created by haojun on 2018/1/11.
  */
class RealtimeFraudClickFilter(conf: _Filter) extends RuleFilter(conf) {
  var jobConf: JobIni = null

  def init(ssc:StreamingContext, jobConf:JobIni, realVars:RealtimeVariables) = {
    vars = realVars.varsMap
    this.jobConf = jobConf
    initRules()
  }

  override def doFilter(data: RDD[Map[String, Any]]): RDD[Map[String, Any]] = {
    val sc = data.sparkContext

    if (isDebug) {
      println("filter input")
      if (!data.isEmpty()) println(data.first)
    }

    var billData: RDD[Map[String, Any]] = sc.emptyRDD
    val bcJobConf = sc.broadcast(jobConf)
    val bcPolicyId = sc.broadcast(policyId)

    for ((space, (groupKey, sortKey, clickTimeDiffRange), filterSpace) <- rules) {
      val bcSpace = sc.broadcast(space)
      val filteredData = data.filter { row =>
        val mSpace = bcSpace.value
        mSpace.contains(row)
      }

      if (isDebug) {
        println("filted data")
        if (!filteredData.isEmpty()) println(filteredData.first)
      }

      //查询Jimdb库
      billData = billData.union(filteredData.map { row =>

        val client: Cluster = JimdbClient.getCluster(bcJobConf.value) // client应在map中初始化，这样才能在client端使用
        val click_id = row("click_id")
        var policy_id = ""
        if (click_id != null && !click_id.equals("")){
          val jkey = "cheat^cid^" + click_id
          val value_o: Array[Byte] = client.get(jkey.getBytes)
          if (null != value_o) {
            val info: CpsCheatClick.CpsCheatClickInfo.Builder =
              CpsCheatClick.CpsCheatClickInfo.newBuilder(CpsCheatClick.CpsCheatClickInfo.parseFrom(value_o))
            if (info.getPolicyIdList.contains(bcPolicyId.value.toString)){
              policy_id = bcPolicyId.value.toString
            }
          }
        }
        row + ("policy_id" -> policy_id)
      }.filter { row =>
        !row("policy_id").equals("")
      })
    }

    billData.cache
    if (isDebug) {
      println("billData count: " + billData.count)
    }
    billData
  }

}
