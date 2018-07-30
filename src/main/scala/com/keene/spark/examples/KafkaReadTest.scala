package com.keene.spark.examples

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark

object KafkaReadTest extends App with SimpleSpark {

  val arg = ArgumentsParser[KafkaReadArgs](args).parse

  spark.
  readStream.
  format("kafka").
  options(Map(
    "kafka.bootstrap.servers" -> arg.servers,
    "subscribe" -> arg.subscribe
  ))
}

class KafkaReadArgs(
  var servers : String = "",
  var subscribe : String = ""
) extends Arguments
