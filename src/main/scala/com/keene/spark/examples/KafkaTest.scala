package com.keene.spark.examples

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._
import com.keene.kafka.KafkaParam

object KafkaTest extends App with SimpleSpark {

  val arg = ArgumentsParser[KafkaReadArgs](args)

  val readParam = KafkaParam( arg.brokers , subscribe = arg.subscribe)
  val kafkaData = spark fromKafka readParam

  kafkaData createOrReplaceTempView "t"


  val writeParam = KafkaParam( arg.brokers , topic = arg.topic)

  "select * from t".go toKafka writeParam start


  spark.streams.awaitAnyTermination

  override def sparkConfOpts: Map[String, String] = super.sparkConfOpts ++ Map(
    "spark.sql.streaming.checkpointLocation" -> "e:/tmp/spark"
  )
}

class KafkaReadArgs(
  var brokers: String = "",
  var subscribe : String = "" ,
  var topic: String = ""
) extends Arguments
