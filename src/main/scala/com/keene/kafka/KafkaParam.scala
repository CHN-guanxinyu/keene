package com.keene.kafka

/**
  * 默认类型为读取
  */
object KafkaParam{
  def apply[T >: KafkaParam](
    brokers : String,
    topic : String,
    as: String = "reader",
    extraOpts : Map[String,String] = Map.empty
  ): T = as match {
    case "reader" => KafkaReaderParam(brokers, topic , extraOpts)
    case "writer" => KafkaWriterParam(brokers, topic , extraOpts)
    case _ => throw new ClassNotFoundException(s"Kafka${as capitalize}Param")
  }
}

sealed abstract class KafkaParam(
  brokers : String ,
  extraOpts : Map[String,String]
) {
  protected def opts : Map[String, String]
  def get : Map[String, String] = Map( "kafka.bootstrap.servers" -> brokers ) ++ opts ++ extraOpts
}

case class KafkaReaderParam(
  brokers : String = "",
  subscribe : String = "",
  extraOpts : Map[String,String] = Map.empty
) extends KafkaParam( brokers , extraOpts ){

  override def opts = Map(
    "subscribe" -> subscribe
  )

}

case class KafkaWriterParam(
  brokers : String = "",
  topic : String = "",
  extraOpts : Map[String,String] = Map.empty
) extends KafkaParam( brokers , extraOpts ){

  override def opts = Map(
    "topic" -> topic
  )

}