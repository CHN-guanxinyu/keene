package com.jd.ad.anti.cps.realtime

//import com.jd.ads.etl.ad.checkpoint.{CheckPoint, Status}
//import kafka.message.MessageAndMetadata
//import kafka.serializer.DefaultDecoder
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.DStream
//import org.slf4j.{Logger, LoggerFactory}
//import com.jd.ad.anti.cps.util.JobIni
//import com.jd.jmq.client.connection.TransportConfig
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import scala.util.Try
//import org.apache.spark.broadcast.Broadcast
//import java.util.Properties
//import org.apache.kafka.common.serialization.StringSerializer
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies
//import org.apache.spark.streaming.kafka010.ConsumerStrategies
//import org.apache.spark.streaming.kafka010.Subscribe
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.kafka.clients.consumer.ConsumerRecord

object DStreamUtils3 {
//  val log: Logger = LoggerFactory.getLogger(this.getClass)
//
//  def createKafkaDStream[K, V](ssc: StreamingContext, config: JobIni, topics: String): InputDStream[ConsumerRecord[K, V]] = {
//    if (config.isCheckPointEnable() && config.isRestoreFromCheckPoint()) {
//      val status = CheckPoint.restoreStatus(config)
//      if(status.offsetRangeList.isEmpty){ // for the first launch
//        createNewKafkaDStream(ssc, config, topics)
//      } else {
//        restoreKafkaDStream(ssc, config, topics, status)
//      }
//    } else {
//      createNewKafkaDStream(ssc, config, topics)
//    }
//  }
//
//  def createNewKafkaDStream[K, V](ssc: StreamingContext, config: JobIni, topics: String):
//  InputDStream[ConsumerRecord[K, V]] = {
//    val topicSet = topics.split(",").toSet
//    var params = Map[String, String]()
//    if (Try(config.getKafkaBrokers).isSuccess) {
//      params = params.+("bootstrap.servers" -> config.getKafkaBrokers)
//    }
//    if (Try(config.getKafkaZookeeperConnect).isSuccess) {
//        params = params.+("zookeeper.connect" -> config.getKafkaZookeeperConnect)
//    }
//    if (Try(config.getKafkaGroupId).isSuccess) {
//        params = params.+("group.id" -> config.getKafkaGroupId)
//    }
//    if (Try(config.getKafkaKeyDeserializer).isSuccess) {
//        params = params.+("key.deserializer" -> config.getKafkaKeyDeserializer)
//    }
//    if (Try(config.getKafkaValueDeserializer).isSuccess) {
//        params = params.+("value.deserializer" -> config.getKafkaValueDeserializer)
//    }
//    if (Try(config.getKafkaSecurityMechanism).isSuccess) {
//      params = params.+("sasl.mechanism" -> config.getKafkaSecurityMechanism)
//    }
//    if (Try(config.getKafkaSecurityProtocol).isSuccess) {
//      params = params.+("security.protocol" -> config.getKafkaSecurityProtocol)
//    }
//    log.warn("Start a new Kafka DStream")
//    KafkaUtils.createDirectStream(
//        ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Subscribe[K, V](topicSet, params)
//    )
//  }
//
//  def restoreKafkaDStream[K, V](ssc: StreamingContext, config: JobIni, topics: String, status: Status):
//  InputDStream[ConsumerRecord[K, V]] = {
//    val topicSet = topics.split(",").toSet
//    var params = Map[String, String]()
//    if (Try(config.getKafkaBrokers).isSuccess) {
//      params = params.+("bootstrap.servers" -> config.getKafkaBrokers)
//    }
//    if (Try(config.getKafkaZookeeperConnect).isSuccess) {
//        params = params.+("zookeeper.connect" -> config.getKafkaZookeeperConnect)
//    }
//    if (Try(config.getKafkaGroupId).isSuccess) {
//        params = params.+("group.id" -> config.getKafkaGroupId)
//    }
//    if (Try(config.getKafkaKeyDeserializer).isSuccess) {
//        params = params.+("key.deserializer" -> config.getKafkaKeyDeserializer)
//    }
//    if (Try(config.getKafkaValueDeserializer).isSuccess) {
//        params = params.+("value.deserializer" -> config.getKafkaValueDeserializer)
//    }
//    if (Try(config.getKafkaSecurityMechanism).isSuccess) {
//      params = params.+("sasl.mechanism" -> config.getKafkaSecurityMechanism)
//    }
//    if (Try(config.getKafkaSecurityProtocol).isSuccess) {
//      params = params.+("security.protocol" -> config.getKafkaSecurityProtocol)
//    }
//    val offsetInfo = status.offsetRangeList.map(x=>x.toString()).mkString(";")
//
//    log.warn("restore from last batch:" + status.getBatchTime() + ",topic info:" + offsetInfo)
//    KafkaUtils.createDirectStream(
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[K, V](topicSet, params, status.getKafkaOffsets(topicSet)))
////    KafkaUtils.createDirectStream[Array[Byte], Array[Byte],
////      DefaultDecoder, DefaultDecoder, (Array[Byte], Array[Byte])](
////      ssc, params, status.getKafkaOffsets(topicSet),
////      (f:MessageAndMetadata[Array[Byte], Array[Byte]])=>(f.key(),f.message))
//  }
//
//  def createKafkaProducer[K, V](ssc: StreamingContext, config: JobIni): Broadcast[KafkaSink[K, V]] = {
//    val kafkaProducerConfig = {
//      var p = new Properties()
//      p.setProperty("bootstrap.servers", config.getKafkaBrokers)
//      p.setProperty("key.serializer", config.getKafkaKeySerializer)
//      p.setProperty("value.serializer", config.getKafkaValueSerializer)
//      p.setProperty("zookeeper.connect", config.getKafkaZookeeperConnect)
//      p.setProperty("group.id", config.getKafkaGroupId)
//      p.setProperty("security.protocol", config.getKafkaSecurityProtocol)
//      p.setProperty("sasl.mechanism", config.getKafkaSecurityMechanism)
//      p.setProperty("auto.create.topics.enable", "true")
//      p
//    }
//    log.warn("kafka producer init done!kafkaProducerConfig:" + kafkaProducerConfig)
//    ssc.sparkContext.broadcast(KafkaSink[K, V](kafkaProducerConfig))
//  }
//
//  def createNewJMQDStream(ssc: StreamingContext, conf: JobIni, storageLevel: StorageLevel): ReceiverInputDStream[String] = {
//    val topic = conf.getJobJMQConsumerTopic()
//    val config: TransportConfig = new TransportConfig()
//    config.setApp(conf.getJobJMQApp())
//    //设置broker地址
//    config.setAddress(conf.getJobJMQAddress())
//    //设置用户名
//    config.setUser(conf.getJobJMQUser())
//    //设置密码
//    config.setPassword(conf.getJobJMQPassword())
//    //设置发送超时
//    config.setSendTimeout(conf.getJobJMQSendtimeout())
//    //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
//    config.setEpoll(conf.getJobJMQEpoll())
//    val receiverInputDStream = ssc.receiverStream(new ReceiverJMQ(topic, config, storageLevel))
//    receiverInputDStream
//  }
}
