package com.jd.ad.anti.cps.dumper

import com.jd.ad.anti.cps.util.{JimdbClient, JobIni}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.jd.ad.anti.cps.entity.CpsCheatClick
import com.jd.ad.anti.cps.realtime.Statistic
import java.util.concurrent.TimeUnit

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by haojun on 2017/10/9.
  */
class RealtimeAntiClickLog extends RealtimeDataDumper{
  @transient
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def saveData(ssc:StreamingContext, dataStream: DStream[Map[String,Any]], conf:JobIni, statistic: Statistic): Unit = {
    dataStream.map { row =>
      val client = JimdbClient.getCluster(conf)
      try {
        val clickid: String = row.getOrElse("click_id", "").toString()
        val jkey = "cheat^cid^" + clickid
        val value_o: Array[Byte] = client.get(jkey.getBytes)
        var info: CpsCheatClick.CpsCheatClickInfo.Builder = null
        //相关policy—id +1，已迁移到订单实时流中统计
//        statistic.updateTotalRecords(row.getOrElse("policy_id","").toString, 1)

        if (null != value_o) {
          info = CpsCheatClick.CpsCheatClickInfo.newBuilder(CpsCheatClick.CpsCheatClickInfo.parseFrom(value_o))
          if (info.getPolicyIdList.contains(row.getOrElse("policy_id", "").toString())) {
            // 不需做操作
          } else {
              info.addPolicyId(row.getOrElse("policy_id", "").toString())
              client.setEx(jkey.getBytes, info.build().toByteArray(), 15, TimeUnit.DAYS)// 15天
          }
        } else {
          info = CpsCheatClick.CpsCheatClickInfo.newBuilder()
          info.setClickId(clickid)
          info.addPolicyId(row.getOrElse("policy_id", "").toString())
          info.setClickTime(row.getOrElse("click_seconds", "").toString().toLong)
          client.setEx(jkey.getBytes, info.build().toByteArray(), 15, TimeUnit.DAYS)// 15天
        }
      } catch {
        case e: Exception => statistic.updateErrorMessage("click_job", "jim db ERROR or CpsCheatClickInfo parse ERROR!!! \n")
//          log.error("Set fraud click error.")
      }

      row("click_id") + "\t" + row("policy_id") + "\t" + row("click_seconds") + "\t" + row("union_id") + "\t" + row("refer") +
       "\t" + row("ad_traffic_type") + "\t" + row("platform")
    }.repartition(1).saveAsTextFiles(conf.getUnionFraudClickOutputPath()+"/secondly")


    this.checkStopCmd(ssc, dataStream, conf, statistic)
  }

}
