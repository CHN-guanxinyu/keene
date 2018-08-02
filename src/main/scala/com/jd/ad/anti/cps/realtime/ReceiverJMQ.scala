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
/**
 * 接入jmq
 */
class ReceiverJMQ(topic: String, config: TransportConfig, storageLevel: StorageLevel)
    extends Receiver[String](storageLevel) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver-" + topic) {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var messageConsumer: MessageConsumer = null

    val consumerConfig: ConsumerConfig = new ConsumerConfig()
    val manager: TransportManager = new ClusterTransportManager(config)
    messageConsumer = new MessageConsumer(consumerConfig, manager, null)
    messageConsumer.start()
    // 拉取消息
    while (!isStopped) {
      messageConsumer.pull(topic, new MessageListener() {
        @throws(classOf[Exception])
        override def onMessage(messages: java.util.List[Message]) = {
          for (i <- 0 until messages.size()) {
            store(messages.get(i).getText)
          }
        }
      })
    }
  }
}