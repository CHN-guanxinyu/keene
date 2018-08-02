package com.jd.ad.anti.cps.realtime

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import com.jd.jmq.client.connection.ClusterTransportManager
import com.jd.jmq.client.connection.TransportConfig
import com.jd.jmq.client.connection.TransportManager
import com.jd.jmq.client.consumer.ConsumerConfig
import com.jd.jmq.client.consumer.MessageConsumer
import com.jd.jmq.client.consumer.MessageListener
import com.jd.jmq.common.message.Message
import org.apache.spark.streaming.StreamingContext
import com.jd.ad.anti.cps.util.JobIni
import org.apache.spark.broadcast.Broadcast

/**
 * 从jmq拉取数据到kafka
 */
class JMQMessageToKafka(jmq_topic: String, kafka_topics: String, 
    kafkaProducer: Broadcast[KafkaSink[String, String]], conf: JobIni, storageLevel: StorageLevel)
    extends Receiver[String](storageLevel) {

  def onStart() {
    println("aaaaaaaaa:Socket JMQMessageToKafka onStart()")
    // Start the thread that receives data over a connection
    new Thread("Socket JMQMessageToKafka") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    // jmq
    val config_jmq: TransportConfig = new TransportConfig()
    config_jmq.setApp(conf.getJobJMQApp())
    // 设置broker地址
    config_jmq.setAddress(conf.getJobJMQAddress())
    // 设置用户名
    config_jmq.setUser(conf.getJobJMQUser())
    // 设置密码
    config_jmq.setPassword(conf.getJobJMQPassword())
    // 设置发送超时
    config_jmq.setSendTimeout(conf.getJobJMQSendtimeout())
    // 设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
    config_jmq.setEpoll(conf.getJobJMQEpoll())
    var messageConsumer: MessageConsumer = null
    val consumerConfig: ConsumerConfig = new ConsumerConfig()
    val manager: TransportManager = new ClusterTransportManager(config_jmq)
    messageConsumer = new MessageConsumer(consumerConfig, manager, null)
    messageConsumer.start()

    // 拉取消息
    while (!isStopped) {
      messageConsumer.pull(jmq_topic, new MessageListener() {
        @throws(classOf[Exception])
        override def onMessage(messages: java.util.List[Message]) = {
          for (i <- 0 until messages.size()) {
            kafkaProducer.value.send(kafka_topics, messages.get(i).getText)
            store(messages.get(i).getText)
          }
        }
      })
    }
  }
}