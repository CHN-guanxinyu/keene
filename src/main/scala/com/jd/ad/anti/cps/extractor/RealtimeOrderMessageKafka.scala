package com.jd.ad.anti.cps.extractor

import java.net.{MalformedURLException, URL}

import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import com.jd.ad.anti.cps.util.{JimdbClient, JobIni}
import com.jd.ad.anti.cps.entity._

import scala.collection.mutable.Set
import com.jd.ad.anti.cps.realtime.CheckPoint
import com.jd.ad.anti.cps.job.RealtimeJobExecutor.getCount
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import com.jd.ad.anti.cps.realtime.DStreamUtils
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges
import com.jd.ad.anti.cps.realtime.Statistic
import com.jd.jim.cli.Cluster
import java.util.concurrent.TimeUnit

import com.jd.ad.anti.cps.entity.UnionClickLogProto.UnionClickLog.ClickLogInfo
import org.slf4j.{Logger, LoggerFactory}

class RealtimeOrderMessageKafka extends RealtimeDataExtractor {

  @transient
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  /** 3号集群
  def getDStream(ssc: StreamingContext, conf: JobIni, counter: JobCounter): DStream[Map[String, Any]] = {
    
    System.setProperty("java.security.auth.login.config", conf.getKafkaSecurityAuthLoginConfig)
    
    val orderStreamKV = DStreamUtils3.createKafkaDStream[Array[Byte], Array[Byte]](ssc, conf, conf.getKafkaOrderTopics())

    var clkOffsetRange = Array[OffsetRange]()

    val orderStream = orderStreamKV.transform { (rdd, t: Time) =>
      clkOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      CheckPoint.storeStatus(conf, clkOffsetRange, t.milliseconds - conf.getJobFrequency() * 1000)
      counter.clean()
      counter.updateTotalRecords(getCount(clkOffsetRange))
      println("-------------------> Fetch order offsets: " + counter.getTotalRecords)
      var res = rdd.flatMap { row =>
        var streamBuilder: SiriusProto.Stream  = null
        val orderSubmitMsg = new OrderSubmitMsg()
        var clickData: Array[Map[String, Any]] = Array()
        try {
          streamBuilder = SiriusProto.Stream.parseFrom(row.value())
          val data : OrderProto.OrderInfo = OrderProto.OrderInfo.parseFrom(streamBuilder.getDataList(0))
          orderSubmitMsg.parse(new String(data.getOrderXml.toByteArray()))
          var clickSet = Set[String]()
          val skuClickInfoList = orderSubmitMsg.getCpsRelatedResult.getSkuClickInfo
          for (i <- 0 until skuClickInfoList.size()) {
            //TODO 应该只需要二跳点击
            List(skuClickInfoList.get(i).getCps_click_log, skuClickInfoList.get(i).getSi_cps_click_log)
              .foreach(info =>
                if (info == null || info.getClickId == null
                  || clickSet.contains(info.getClickId)) {
                  // 无操作
                } else {
                  var unionId = 0L
                  Try {
                    unionId = info.getUnionId.asInstanceOf[Long]
                  }
                  var clickInfo: Map[String, Any] = Map(
                    //TODO 如果clickSystem只能等于2，是否还需要保留？
                    "clickSystem" -> 2,
                    "union_id" -> unionId,
                    "sub_union_id" -> getStringOrElse(info.getcUnionId(), getStringOrElse(info.getCUnionId)),
                    "click_id" -> getStringOrElse(info.getClickId), //"a1d2ee0ac954418f807c129da302ff1f",
                    "mobile_type" -> getIntegerAsInt(info.getPlatform, 2),
                    "site_id" -> getIntegerAsInt(info.getSiteId),
                    "ad_traffic_type" -> getIntegerAsInt(info.getAdTrafficType),
                    "position_id" -> getIntegerAsInt(info.getPositionId),
                    "order_id" -> orderSubmitMsg.getOrderId,
                    "null_sub_union_id" -> "null",
                    "illegal_sub_union_id" -> "#allthesubsite")
                  clickSet += info.getClickId
                  clickData :+= clickInfo
                })
          }
        } catch {
          case e: Exception =>
            counter.updateErrorMessage("Phase order message failed.\n" + e.getMessage)
            println("Cache error: " + e.getMessage)
        }
        Array(Map(
          "orderId" -> orderSubmitMsg.getOrderId,
          "clickInfo" -> clickData,
          "orderXml" -> row))
      }
      res
    }
    orderStream
  }*/
  
  def getDStream(ssc: StreamingContext, conf: JobIni, statistic: Statistic): DStream[Map[String, Any]] = {
          
          val orderStreamKV = DStreamUtils.createKafkaDStream(ssc, conf, conf.getKafkaClickTopics())
          
          var clkOffsetRange = Array[OffsetRange]()
          
          val orderStream = orderStreamKV.transform { (rdd, t: Time) =>
            clkOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            CheckPoint.storeStatus(conf, clkOffsetRange, t.milliseconds - conf.getJobFrequency() * 1000)
            //statistic 更新 inputCnt
            statistic.startJob()

            val res = rdd.map { row =>
              var streamBuilder: SiriusProto.Stream  = null
              var clickData: Array[Map[String, Any]] = Array()
              var orderId : Long = 0L
              var xml : ByteString = null
              var recTime : String = null
              var sendPay : String = ""
              var chaojifanFlag : String = ""
              var hitPolicy : String = ""
              val hitPolicySet : Set[String] = Set[String]()
              try {
                streamBuilder = SiriusProto.Stream.parseFrom(row._2)
                for (i <- 0 until streamBuilder.getDataListCount) {
                  val orderInfo : OrderProto.OrderInfo = OrderProto.OrderInfo.parseFrom(streamBuilder.getDataList(i))
                  if (orderInfo == null) {
                    statistic.updateErrorMessage("order_job", "OrderInfo.parseFrom ERROR!!! \n")
                  } else {
                    if (i == 0) { // 取第一个订单的订单号及xml
                      statistic.updateTotalDelay("order_job", System.currentTimeMillis()/1000 - orderInfo.getCreateDate)  //create_date 订单创建时间(秒) stream中多个orderInfo是一个订单，因为点击不同被拆分
                      statistic.updateTotalRecords("order_job", 1)
                      orderId = orderInfo.getOrderId
                      xml = orderInfo.getOrderXml
                      recTime = orderInfo.getMonitorKey
                      sendPay = orderInfo.getSendPay
                      chaojifanFlag = getChaojifanFlag(sendPay)
                    }
                    var platform: String = "pc"
                    if (orderInfo.getMobileType == 2){
                      platform = "mobile"
                    }
                    if (orderInfo.hasAntiInfo) {
                      for (k <- 0 until orderInfo.getAntiInfo.getHitPolicyCount) {
                        hitPolicySet.add(orderInfo.getAntiInfo.getHitPolicyList.get(k))
                      }
                    }
                    var refer: String = ""
                    var referHost: String = ""
                    var jda: String = ""
                    var unionId: String = "0"
                    var subUnionId: String = ""
                    var createTime: String = ""
                    var skipFlag: String = "false"
                    var clickId: String = ""
                    var siteId: Int = 0
                    var adTrafficType: Int = 0
                    var positionId: Int = 0
                    if (orderInfo.hasClickId && !orderInfo.getClickId.trim().equals("")) {
                      val clickInfo = getClickInfo(orderInfo.getClickId,conf)
                      if (clickInfo.isEmpty) {
                        skipFlag = "true"
                      }
                      unionId = clickInfo.getOrElse("union_id", orderInfo.getUnionId).toString
                      subUnionId = clickInfo.getOrElse("sub_union_id", orderInfo.getSubUnionId).toString
                      createTime = clickInfo.getOrElse("create_time", "").toString
                      refer = clickInfo.getOrElse("refer", "").toString
                      try{
                        referHost = new URL(refer).getHost
                      } catch {
                        case ex: MalformedURLException =>
                          referHost = refer
                      }
                      clickId = orderInfo.getClickId
                      siteId = orderInfo.getSiteId
                      adTrafficType = orderInfo.getAdTrafficType
                      positionId = orderInfo.getPositionId   //clickId,siteId,adTrafficType,positionId均来自CpsRelatedResult的sku节点的cps_click_log或Si_cps_click_log，应该在clickid有效时才具有有效值
                      jda = clickInfo.getOrElse("jda", "").toString
                    }else{
                      skipFlag = "true"
                    }

                    var clickInfo: Map[String, Any] = Map(
                      "union_id" -> unionId,
                      "sub_union_id" -> subUnionId,
                      "null_sub_union_id" -> "null",
                      "illegal_sub_union_id" -> "#allthesubsite",
                      "click_id" -> clickId, //"a1d2ee0ac954418f807c129da302ff1f",
                      "site_id" -> siteId,
                      "ad_traffic_type" -> adTrafficType,
                      "position_id" -> positionId,
                      "order_id" -> orderInfo.getOrderId,
                      "platform" -> platform,
                      "refer" -> refer,
                      "refer_host" -> referHost,
                      "jda" -> jda,
                      "create_time" -> createTime,
                      "chaojifan_flag" -> chaojifanFlag,
                      "skip_flag" -> skipFlag)

                    clickData :+= clickInfo // 同一个订单号的多个点击合并到一块

                  }
                }
              } catch {
                case e: Exception => statistic.updateErrorMessage("order_job", "Phase order message failed. \n")
              }
              hitPolicySet.foreach( (f:String) => {
                if (hitPolicy == "") {
                  hitPolicy = f
                } else {
                  hitPolicy = hitPolicy + "," + f
                }
              })
              // 检查漏单使用
              // val jimClient: Cluster = JimdbClient.getCluster(conf)
              // jimClient.sAdd("ss_rec_" + recTime, "" + orderId)
              // jimClient.expire("ss_rec_" + recTime, 1, TimeUnit.DAYS) // 漏单统计保留1天
              Map(
                "orderId" -> orderId,
                "clickInfo" -> clickData,
                "orderXml" -> xml.toStringUtf8,
                "recTime" -> recTime,
                "sendPay" -> sendPay,
                "hitPolicy" -> hitPolicy)
            }
            res
          }
          orderStream

//    getTestOrderRDD(ssc,conf)
  }

  def getChaojifanFlag(sendPay: String) : String = {
    var chaojifanFlag: String = ""
    // 超级返的联盟ID=1000352685 ,send_pay打标规则：60-62位 033
    if (sendPay.size >= 62)
      chaojifanFlag = sendPay.substring(59,62)
    chaojifanFlag
  }

  def getClickInfo(clickId: String, conf: JobIni) : scala.collection.mutable.Map[String,Any] = {
    var ret: scala.collection.mutable.Map[String,Any] =  scala.collection.mutable.Map()
    // 由于jimdb库资源的问题，旁路使用线上的库来获取点击
    val client: Cluster = JimdbClient.getCluster(conf, "online")
    val key = conf.getClickPrefix + clickId
    val value_o: Array[Byte] = client.get(key.getBytes)
    if (null != value_o) {
      try {
        val clickInfo: ClickLogInfo.Builder =
          ClickLogInfo.newBuilder(ClickLogInfo.parseFrom(value_o))
        ret += ("sid" -> clickInfo.getClickId)
        ret += ("jda" -> clickInfo.getJdaUid)
        ret += ("refer" -> clickInfo.getRefer)
        ret += ("sub_union_id" -> clickInfo.getCUnionId)
        ret += ("union_id" -> clickInfo.getUnionId)
        ret += ("create_time" -> clickInfo.getCreatetime)
      } catch {
        case ex: InvalidProtocolBufferException =>
        //counter.updateErrorMessage("Union click log proto parse failed!")
      }
    }
    ret
  }

  def getIntegerAsInt(v: Integer, defaultValue: Int = 0): Int = {
    if (v != null) v.intValue() else defaultValue
  }

  def getTestOrderRDD(ssc: StreamingContext, conf: JobIni): DStream[Map[String, Any]] = {
    var rowData: Array[Map[String, Any]] = Array()
    val order_xml =
      """
        |
      """.stripMargin
    var clickData: Array[Map[String, Any]] = Array()
    // fraud click
    clickData :+= Map(
      "union_id" -> 316L,
      "sub_union_id" -> "21640",
      "null_sub_union_id" -> "null",
      "illegal_sub_union_id" -> "#allthesubsite",
      "click_id" -> "3ab654faa0be4cd9a4fa9b388a34864a",
      "site_id" -> "",
      "ad_traffic_type" -> 6,
      "position_id" -> 0,
      "order_id" -> 71176150260L,
      "platform" -> "pc",
      "refer" -> "http://www.5566.net/main.htm",
      "refer_host" -> "www.5566.net",
      "jda" -> "696831364",
      "create_time" -> "2018-02-07 23:45:06",
      "chaojifan_flag" -> "",
      "skip_flag" -> "false")
    rowData :+= Map(
      "orderId" -> 71176150260L,
      "clickInfo" -> clickData,
      "orderXml" -> order_xml,
      "recTime" -> "",
      "sendPay" -> "000",
      "hitPolicy" -> "")


    clickData :+= Map(
      "union_id" -> 36378L,
      "sub_union_id" -> "327343_tgYjJl",
      "null_sub_union_id" -> "null",
      "illegal_sub_union_id" -> "#allthesubsite",
      "click_id" -> "1c9cd11f309645e68f35c18a2d10a7ca",
      "site_id" -> "",
      "ad_traffic_type" -> 6,
      "position_id" -> 0,
      "order_id" -> 71176419517L,
      "platform" -> "pc",
      "refer" -> "http://p.yiqifa.com/l?l=Cl7SYKqBR9eQgyUxU7qSpNM2W9sWgQ4IPQzsKN37POsWRNMxWy6Q37qxP7PpYnMd6wzOgKgVRwwOpByCNtPSYyPmP7HSMPRep7MSpPRV49KoUJUMCwAx496sRc4yYcgNYmPS3OKo3SM7!5b7UZjvfSol35qyf9Awf96_Ypob3mD_UN4uRBU!kBt7N9yqUIeuUJsx",
      "refer_host" -> "p.yiqifa.com",
      "jda" -> "1514477714944302199141",
      "create_time" -> "2018-02-05 19:47:43",
      "chaojifan_flag" -> "",
      "skip_flag" -> "false")
    rowData :+= Map(
      "orderId" -> 71176419517L,
      "clickInfo" -> clickData,
      "orderXml" -> order_xml,
      "recTime" -> "",
      "sendPay" -> "000",
      "hitPolicy" -> "")


    clickData :+= Map(
      "union_id" -> 9998189L,
      "sub_union_id" -> "52354",
      "null_sub_union_id" -> "null",
      "illegal_sub_union_id" -> "#allthesubsite",
      "click_id" -> "7e7e059a4ea84c169896f8286e9018f6",
      "site_id" -> "",
      "ad_traffic_type" -> 6,
      "position_id" -> 0,
      "order_id" -> 71201388056L,
      "platform" -> "pc",
      "refer" -> "http://union-click.jd.com/jdc?e=&p=AyIPZRprFDJWWA1FBCVbV0IUEEULXg1cAAQJS14idQZlJmpBb1kwYBNxQGpkJRpSVkAad1kXNRYDEg5JGlgJAxsWVBNQFwoZA1QrXBAEEA5TG1glChoPXBtSHTIWBFcfXiUCEzcedVolAyIHURtSFAYQD10aXRcKIgBlztKk25aBgqnKJTIiN2UrWxUyEg%3D%3D&t=W1dCFBBFC14NXAAECUte",
      "refer_host" -> new URL("http://union-click.jd.com/jdc").getHost,
      "jda" -> "1514477714944302199141",
      "create_time" -> "2018-02-08 09:39:50",
      "chaojifan_flag" -> "",
      "skip_flag" -> "false")
    rowData :+= Map(
      "orderId" -> 71201388056L,
      "clickInfo" -> clickData,
      "orderXml" -> order_xml,
      "recTime" -> "",
      "sendPay" -> "000",
      "hitPolicy" -> "")

    val testDStream = new ConstantInputDStream(ssc, ssc.sparkContext.parallelize(rowData))
      .window(Seconds(conf.getJobFrequency()), Seconds(conf.getJobFrequency()))

    testDStream
  }
}
