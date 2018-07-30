package com.keene.spark.examples

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
object KafkaReadTest extends App with SimpleSpark {

  val arg = ArgumentsParser[KafkaReadArgs](args)

  val kafkaDF = spark.
    readStream.
    format("kafka").
    options(Map(
      "kafka.bootstrap.servers" -> arg.servers,
      "subscribe" -> arg.subscribe
    )).load

  kafkaDF.
  selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").
  writeStream.
  format("console").
  start

  spark.streams.awaitAnyTermination

}

class KafkaReadArgs(
                     var servers: String = "",
                     var subscribe: String = ""
                   ) extends Arguments
