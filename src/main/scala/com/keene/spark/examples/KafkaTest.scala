package com.keene.spark.examples

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._
import com.keene.kafka.KafkaParam

object KafkaTest extends App with SimpleSpark {

  val arg = ArgumentsParser[KafkaReadArgs](args)

  implicit val kafkaParam: KafkaParam = KafkaParam("localhost:9092" , "test")

  val kafkaDF = spark.fromKafka

  kafkaDF.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").
    toKafka.start

  spark.streams.awaitAnyTermination

  override def sparkConfOpts: Map[String, String] = super.sparkConfOpts ++ Map(
    "spark.sql.streaming.checkpointLocation" -> "e:/tmp/spark"
  )
}

class KafkaReadArgs(
  var brokers: String = "",
  var topics: String = ""
) extends Arguments
