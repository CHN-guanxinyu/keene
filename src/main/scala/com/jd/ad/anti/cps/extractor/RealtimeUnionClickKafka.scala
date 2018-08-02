package com.jd.ad.anti.cps.extractor

import java.net.{MalformedURLException, URL}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import com.google.protobuf.InvalidProtocolBufferException
import com.jd.ad.anti.cps.entity.UnionClickLogProto
import com.jd.ad.anti.cps.entity.UnionClickLogProto.UnionClickKafkaLog
import com.jd.ad.anti.cps.entity.UnionClickLogProto.UnionClickLog.ClickLogInfo
import com.jd.ad.anti.cps.job.RealtimeJobExecutor.getCount
import com.jd.ad.anti.cps.realtime.{CheckPoint, JobCounter, Statistic}
import com.jd.ad.anti.cps.util.{JimdbClient, JobIni}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try
import com.jd.ad.anti.cps.realtime.DStreamUtils
import com.jd.jim.cli.Cluster
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * Created by haojun on 2017/9/7.
  */
class RealtimeUnionClickKafka extends RealtimeDataExtractor{
  @transient
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def getDStream(ssc:StreamingContext,
                          conf:JobIni,
                          statistic:Statistic) : DStream[Map[String,Any]] = {
    val clkStreamKV = DStreamUtils.createKafkaDStream(ssc, conf, conf.getKafkaClickTopics())

    var clkOffsetRange = Array[OffsetRange]()

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val clkStream = clkStreamKV.transform { (rdd, t: Time) =>
      statistic.startJob()
//      Add checkpoint
      clkOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      CheckPoint.storeStatus(conf, clkOffsetRange, t.milliseconds - conf.getJobFrequency() * 1000)

//      println("-------------------> Fetch click offsets: " + statistic)
//      println("-------------------> Fetch statistic counters: " + statistic.getCounters.toString)
      rdd.flatMap { row =>
        var rowData: Array[Map[String, Any]] = Array()
        var unionClickKafkaLog: UnionClickKafkaLog = null
        try {
          unionClickKafkaLog = UnionClickLogProto.UnionClickKafkaLog.parseFrom(row._2)
        } catch {
          case ex: InvalidProtocolBufferException =>statistic.updateErrorMessage("click_job" , "UnionClickKafkaLog parse ERROR!!! \n")
        }
        if (unionClickKafkaLog.hasUnionClickLog) {

          val unionClickLog = unionClickKafkaLog.getUnionClickLog
          for (i <- 0 until unionClickLog.getClickLogInfoCount) {  //一般 unionClickLog.getClickLogInfoCount=1
            val clickLogInfo = unionClickLog.getClickLogInfo(i)
            var referHost = ""
            var targetUrlHost = ""
            var unionId = 0L

            //统计当前时间相对点击时间的延迟
            val delay = System.currentTimeMillis()/1000 - simpleDateFormat.parse(clickLogInfo.getCreatetime).getTime/1000
            statistic.updateTotalDelay("click_job", delay);
            statistic.updateTotalRecords("click_job" , 1)
            // 由于jimdb库资源的问题，旁路不写原始点击
            if (conf.getRunEnv().equalsIgnoreCase("online") && conf.getIsSetClick())
              setClickInfo(clickLogInfo, conf)

            try {
              referHost = new URL(clickLogInfo.getRefer).getHost
            } catch {
              case ex: MalformedURLException =>
                referHost = clickLogInfo.getRefer
            }

            try {
              targetUrlHost = new URL(clickLogInfo.getTu).getHost
            } catch {
              case ex: MalformedURLException =>
                targetUrlHost = clickLogInfo.getTu
            }

            Try {
              unionId = clickLogInfo.getUnionId.asInstanceOf[Long]
            }

            val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dt2 = fm.parse(clickLogInfo.getCreatetime)
            val clickSeconds:Long = dt2.getTime / 1000L
            val platform = if (clickLogInfo.getPlatform == 1) "pc" else "mobile"

            rowData :+= Map(
              "click_id" -> clickLogInfo.getClickId,
              "refer" -> clickLogInfo.getRefer,
              "target_url" -> clickLogInfo.getTu,
              "click_time" -> clickLogInfo.getCreatetime,
              "click_seconds" -> clickSeconds,
              "union_id" -> unionId,
              "sub_union_id" -> clickLogInfo.getCUnionId,
              "platform" -> platform,
              "spread_type" -> clickLogInfo.getAdSpreadType,
              "ad_traffic_type" -> clickLogInfo.getAdTrafficType,
              "click_ip" -> clickLogInfo.getIp,
              "ua" -> clickLogInfo.getUa,
              "jda" -> clickLogInfo.getJdaUid,
              "csid" -> clickLogInfo.getCSID,
              "refer_host" -> referHost,
              "target_url_host" -> targetUrlHost
            )
          }
        }
        rowData
      }
    }

    clkStream

//    getTestHttpRDD(ssc, conf)
//    getTestDnsRDD(ssc, conf)

  }

  def setClickInfo(clickInfo : UnionClickLogProto.UnionClickLog.ClickLogInfo, conf : JobIni) : Unit = {
    val client: Cluster = JimdbClient.getCluster(conf)
    val key = conf.getClickPrefix + clickInfo.getClickId
    try {
      val newClickInfo: ClickLogInfo.Builder = ClickLogInfo.newBuilder()
      newClickInfo.setClickId(clickInfo.getClickId)
      newClickInfo.setJdaUid(clickInfo.getJdaUid)
      newClickInfo.setRefer(clickInfo.getRefer)
      newClickInfo.setCUnionId(clickInfo.getCUnionId)
      newClickInfo.setUnionId(clickInfo.getUnionId)
      newClickInfo.setCreatetime(clickInfo.getCreatetime)
      client.set(key.getBytes, newClickInfo.build().toByteArray)
      client.expire(key.getBytes, conf.getClickExpireTime(), TimeUnit.SECONDS)
    } catch {
      case ex: InvalidProtocolBufferException =>
//        log.error("Set click info failed.")
    }
  }

  def getTestHttpRDD(ssc:StreamingContext, conf:JobIni) : DStream[Map[String,Any]] = {
    var rowData: Array[Map[String, Any]] = Array()
//    # 过滤京乐享和京享街(17, 44),target_url不匹配判作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d7",
      "refer" -> "norefer",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 326418457L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 17,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (Windows; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "",
      "target_url_host" -> "re.m.jd.com"
    )
//    # 过滤个人推广工具(31,117) or 微信\手Q通过API接口领取短链接(142), refer不匹配判作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d6",
      "refer" -> "",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 326418457L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 31,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (Windows; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "",
      "target_url_host" -> "re.m.jd.com"
    )
//    # 过滤CPS流量池中间页点击(75), refer不匹配判作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d5",
      "refer" -> "",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 326418457L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 3,
      "ad_traffic_type" -> 75,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (Windows; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "",
      "target_url_host" -> "re.m.jd.com"
    )
//    # 过滤社交媒体推广(32), refer与备案不符判作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d4",
      "refer" -> "",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 303588831L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 32,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (Windows; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "test.com",
      "target_url_host" -> "re.m.jd.com"
    )
//    # 对有refer的url进行处理, refer与备案不符判作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d3",
      "refer" -> "",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 3338018L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 75,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (Windows; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "test.com",
      "target_url_host" -> "re.m.jd.com"
    )
//    # 对第三方联盟为refer和norefer且union_id不在白名单中的全部算作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d2",
      "refer" -> "norefer",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 50200005L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 99,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (Windows; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "test.com",
      "target_url_host" -> "re.m.jd.com"
    )

    val testDStream = new  ConstantInputDStream(ssc, ssc.sparkContext.parallelize(rowData))
      .window(Seconds(conf.getJobFrequency()),Seconds(conf.getJobFrequency()))

    testDStream
  }



  def getTestDnsRDD(ssc:StreamingContext, conf:JobIni) : DStream[Map[String,Any]] = {
    var rowData: Array[Map[String, Any]] = Array()
//    # jda 用于标识同一用户，如果联盟切换则作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d7",
      "refer" -> "norefer",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:41",
      "click_seconds" -> 1507439741L,
      "union_id" -> 326418457L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 1,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "",
      "target_url_host" -> "re.m.jd.com"
    )

    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d7",
      "refer" -> "norefer",
      "target_url" -> "http://re.m.jd.com/list/item/1195-12203474477.html",
      "click_time" -> "2017-10-08 13:15:51",
      "click_seconds" -> 1507439751L,
      "union_id" -> 326418458L,
      "sub_union_id" -> "qqbrowsermz",
      "platform" -> "pc",
      "spread_type" -> 2,
      "ad_traffic_type" -> 1,
      "click_ip" -> "163.204.6.195",
      "ua" -> "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_2 like Mac OS X)",
      "jda" -> "657883567",
      "csid" -> "3ff324d8fa0f4d359a68016e232e804b^1000012192^2017-10-08 13:16:18^117.136.70.175",
      "refer_host" -> "",
      "target_url_host" -> "re.m.jd.com"
    )

    val testDStream = new  ConstantInputDStream(ssc, ssc.sparkContext.parallelize(rowData))
      .window(Seconds(conf.getJobFrequency()),Seconds(conf.getJobFrequency()))

    testDStream
  }
}
