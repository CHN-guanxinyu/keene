package com.jd.ad.anti.cps.realtime

//import com.jd.ads.etl.ad.checkpoint.{CheckPoint, Status}
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.{Logger, LoggerFactory}
import com.jd.ad.anti.cps.util.JobIni
import com.jd.jmq.client.connection.TransportConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Created by liujun on 2017/1/6.
  */
object DStreamUtils {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def createKafkaDStream(ssc: StreamingContext, config: JobIni, topics: String): DStream[(Array[Byte], Array[Byte])] = {
    if (config.isCheckPointEnable() && config.isRestoreFromCheckPoint()) {
      val status = CheckPoint.restoreStatus(config)
      if(status.offsetRangeList.isEmpty){ // for the first launch
        createNewKafkaDStream(ssc, config, topics)
      } else {
        restoreKafkaDStream(ssc, config, topics, status)
      }
    } else {
      createNewKafkaDStream(ssc, config, topics)
    }
  }

  def createNewKafkaDStream(ssc: StreamingContext, config: JobIni, topics: String):
  DStream[(Array[Byte], Array[Byte])] = {
    val topicSet = topics.split(",").toSet
    val params = Map[String, String]("metadata.broker.list" -> config.getKafkaBrokers)

    log.warn("Start a new Kafka DStream")
    KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, params, topicSet)
  }

  def restoreKafkaDStream(ssc: StreamingContext, config: JobIni, topics: String, status: Status):
  DStream[(Array[Byte], Array[Byte])] = {
    val topicSet = topics.split(",").toSet
    val params = Map[String, String]("metadata.broker.list" -> config.getKafkaBrokers)

    val offsetInfo = status.offsetRangeList.map(x=>x.toString()).mkString(";")

    log.warn("restore from last batch:" + status.getBatchTime() + ",topic info:" + offsetInfo)
    KafkaUtils.createDirectStream[Array[Byte], Array[Byte],
      DefaultDecoder, DefaultDecoder, (Array[Byte], Array[Byte])](
      ssc, params, status.getKafkaOffsets(topicSet),
      (f:MessageAndMetadata[Array[Byte], Array[Byte]])=>(f.key(),f.message))
  }

  
  def createNewJMQDStream(ssc: StreamingContext, conf: JobIni, storageLevel: StorageLevel): ReceiverInputDStream[String] = {
    val topic = conf.getJobJMQConsumerTopic()
    val config: TransportConfig = new TransportConfig()
    config.setApp(conf.getJobJMQApp())
    //设置broker地址
    config.setAddress(conf.getJobJMQAddress())
    //设置用户名
    config.setUser(conf.getJobJMQUser())
    //设置密码
    config.setPassword(conf.getJobJMQPassword())
    //设置发送超时
    config.setSendTimeout(conf.getJobJMQSendtimeout())
    //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
    config.setEpoll(conf.getJobJMQEpoll())
    val receiverInputDStream = ssc.receiverStream(new ReceiverJMQ(topic, config, storageLevel))
    receiverInputDStream
  }
}
