package com.keene.spark.examples.kafka

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.kafka.KafkaParam
import com.keene.spark.examples.main.ExampleRunner
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._
/**
  * 多流聚合
  * 其实跟是否是Kafka无关了
  * Streaming的范围
  */
class Lv2 extends SimpleSpark with ExampleRunner{
  override def run(args: Array[String]): Unit = {
    //TODO : 多流聚合
    val arg = ArgumentsParser[Lv1Args](args)

    val readParam = KafkaParam(arg.brokers , arg.subscribe)
    spark.fromKafka(readParam).writeStream.format("console").start

    spark.streams.awaitAnyTermination
  }
}

