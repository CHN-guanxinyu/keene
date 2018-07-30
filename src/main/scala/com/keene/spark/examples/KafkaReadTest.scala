package com.keene.spark.examples
import com.keene.core.parsers.Arguments
import com.keene.spark.utils.SimpleSpark

object KafkaReadTest extends App with SimpleSpark{
  spark.
    readStream.
    format("kafka").
    options(Map(
      "kafka.bootstrap.host" -> ""
    ))
}

class KafkaReadArgs() extends Arguments
