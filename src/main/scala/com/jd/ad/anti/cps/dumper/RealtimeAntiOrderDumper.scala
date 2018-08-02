package com.jd.ad.anti.cps.dumper

import com.jd.ad.anti.cps.util.{JimdbClient, JobIni}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import java.net.URLEncoder
import java.util.HashMap
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import com.jd.jmq.common.message.Message
import com.jd.purchase.domain.old.bean.Order
import com.jd.purchase.sdk.common.serialize.XmlSerializableTool

import scala.util.Try
import scala.collection.mutable.Set
import com.jd.ad.anti.cps.realtime.MessageProducerJMQ
import com.jd.ad.anti.cps.realtime.Statistic
import com.jd.jim.cli.Cluster
import com.jd.jmq.common.exception.JMQException
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by haojun on 2017/10/9.
 */
class RealtimeAntiOrderDumper extends RealtimeDataDumper {
  @transient
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def saveData(ssc: StreamingContext, dataStream: DStream[Map[String, Any]], conf: JobIni, statistic: Statistic): Unit = {

    dataStream.map( (row: Map[String,Any]) => {
        val isBill: String = row.getOrElse("isBill", "").toString()
        val orderId: String = row.getOrElse("orderId", "").toString()
        var orderXml: String = row.getOrElse("orderXml", "").toString()
        val recXmlTime: String = row.getOrElse("recTime", "").toString()
        val oldHitPolicy: String = row.getOrElse("hitPolicy", "").toString()
        var hitPolicy: String = ""
        var cps_cheat_stat: String = ""
        val hitPolicySet = Set[String]()
        var isPassTest: Int = 0
        var fraudClickId: String = ""

        if ("1" == isBill) {

          val cheatClickMap = new HashMap[String, Integer]()
          var minPolicyId : Int = Int.MaxValue
          row("fraudClickInfo").asInstanceOf[Array[Map[String, Any]]].foreach { row =>
            val click_id = row.getOrElse("click_id", "").toString()
            val p_id = row.getOrElse("policy_id", "")
            hitPolicySet.add(p_id.toString)
            val policy_id: Int = if(null == p_id || Try(p_id.toString().toInt).isFailure) 0 else p_id.toString().toInt
            statistic.updateTotalRecords(policy_id.toString, 1)
            if (0 != policy_id) {
              cheatClickMap.put(click_id, policy_id.toInt)
              if (policy_id.toInt < minPolicyId) {
                minPolicyId = policy_id.toInt // 取最小的policyId
                fraudClickId = click_id
              }
            }
            hitPolicySet.foreach( (f:String) => {
              if (hitPolicy == "") {
                hitPolicy = f
              } else {
                hitPolicy = hitPolicy + "," + f
              }
            })
          }

          statistic.updateTotalRecords("fraud_orders", 1)
          //statistic.updateTotalRecords(minPolicyId.toString, 1)
          cps_cheat_stat = "" + minPolicyId

          val orderAndCartSplit: String = "8e5220aa_bbc2_4eb9_b849_a4afd76bc6e3"
          val orderAndCart: Array[String] = orderXml.split(orderAndCartSplit)
          if (orderAndCart.length != 2) {
            statistic.updateErrorMessage("order_job", "Order XML without split string " + orderAndCartSplit + ". \n")
          } else {
            try {
              val orderMq: Order = XmlSerializableTool.deSerializeXML(classOf[Order], orderAndCart(0), true)
              if (null != orderMq) {
                var extTagMap = orderMq.getExtTags()
                if (null == extTagMap) {
                  extTagMap = new HashMap[String, String]()
                }
                extTagMap.put("cps_cheat_result", URLEncoder.encode(JSON.toJSONString(cheatClickMap, true), "UTF-8"))
                extTagMap.put("cps_cheat_stat", cps_cheat_stat)
                orderMq.setExtTags(extTagMap)
                orderXml = XmlSerializableTool.serializeXML(orderMq, true) + orderAndCartSplit + orderAndCart(1)
              } else {
                statistic.updateErrorMessage("order_job", "Order XML deserialization failed : " + orderAndCart(0) + ". \n")
              }
            } catch {
              case ex: Exception => statistic.updateErrorMessage("order_job", "Order XML deserialization ERROR!!! \n")
            }
          }
        }
        var retry: Int = 0
        val retryTimes : Int = conf.getJobJmqSendRetryTimes()
        while(retry < retryTimes) {
          try {
            val message: Message = new Message(conf.getJobJMQProducerTopic(), orderXml, orderId)
            MessageProducerJMQ.getProducer(conf)
            if (conf.getJobJMQSendEnabled()) {
              MessageProducerJMQ.send(message)
            }
            val jimClient: Cluster = JimdbClient.getCluster(conf)
            val keyPrefix: String = conf.getJimdbOrderKeyPrefix()
            val expireDay: Int = conf.getJimdbOrderExpireDay()
            jimClient.sAdd(keyPrefix + recXmlTime, orderId)
            jimClient.expire(keyPrefix + recXmlTime, expireDay, TimeUnit.DAYS)
            retry = retryTimes // send 成功
          } catch {
            case ex: JMQException => statistic.updateErrorMessage("order_job", "JMQException ERROR!!!" + retry + ex.getMessage + "\n")
              retry = retry + 1
              Thread.sleep(1000)
            case ex: Exception => statistic.updateErrorMessage("order_job", "jmq send exception:" + retry + ex.getMessage + "\n")
              retry = retry + 1
              Thread.sleep(1000);
          }
        }

        if (hitPolicy == "" && oldHitPolicy == "") {
          isPassTest = 0
        }else if (hitPolicy != "" && oldHitPolicy != "") {
          isPassTest = 1
        }else if (hitPolicy != "" && oldHitPolicy == ""){
          isPassTest = 2
        }else if (hitPolicy == "" && oldHitPolicy != ""){
          isPassTest = 3
        }

        row.getOrElse("orderId", "").toString+"\t"+cps_cheat_stat+"\t"+oldHitPolicy+"\t"+isPassTest+"\t"+fraudClickId

    }).repartition(1).saveAsTextFiles(conf.getUnionFraudOrderOutputPath()+"/secondly")

    this.checkStopCmd(ssc, dataStream, conf, statistic)
  }
}
