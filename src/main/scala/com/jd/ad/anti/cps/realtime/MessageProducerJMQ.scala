package com.jd.ad.anti.cps.realtime

import com.jd.jmq.client.producer.MessageProducer
import com.jd.jmq.client.connection.TransportConfig
import com.jd.jmq.client.connection.ClusterTransportManager
import com.jd.ad.anti.cps.util.JobIni
import com.jd.jmq.common.message.Message

object MessageProducerJMQ extends Serializable {

  private var manager: ClusterTransportManager = null
  private var producer: MessageProducer = null
  
  def getProducer(conf: JobIni): MessageProducer = {
    if (manager == null) {
      var config: TransportConfig = new TransportConfig()
      val topic = conf.getJobJMQProducerTopic()
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
      manager = new ClusterTransportManager(config)
      manager.start()
    }
    if (producer == null) {
      while(!manager.isStarted) {
        manager.start()
        println("-------------------> manager no started")
      }
      producer = new MessageProducer(manager)
      producer.start()
    }
    producer
  }
  
  def send(message: Message): Unit = {
    producer.send(message)
  }

  def down() {
    if (manager != null) {
      manager.stop()
    }
    if (producer != null) {
      producer.stop()
    }
  }
}