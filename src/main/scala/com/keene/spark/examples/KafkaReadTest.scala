package com.keene.spark.examples

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark

object KafkaReadTest extends App with SimpleSpark {
  val argv = ArgumentsParser[KafkaReadArgs](args).parse

  println(argv.host , argv.topics)
  /*spark.
  readStream.
  format("kafka").
  options(Map(
    "kafka.bootstrap.host" -> ""
  ))*/
}

class KafkaReadArgs(
  var host : String = "123",
  var topics : String = ""
) extends Arguments
