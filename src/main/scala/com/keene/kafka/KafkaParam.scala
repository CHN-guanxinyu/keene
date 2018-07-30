package com.keene.kafka

case class KafkaParam(
  brokers : String = "",
  topic : String = "",
  subscribe : String = "",
  extraOpts : Map[String,String] = Map.empty )
{
  def opts: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> brokers,
    "subscribe" -> subscribe,
    "topic" -> topic
  ).filter(_._2 != "") ++ extraOpts
}
