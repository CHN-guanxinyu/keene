package com.keene.kafka

case class KafkaParam(
                       brokers : String = "",
                       topic : String = "",
                       extraOpts : Map[String,String] = Map.empty )
{
  def opts: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> brokers,
    "subscribe" -> topic,
    "topic" -> topic
  ) ++ extraOpts
}
