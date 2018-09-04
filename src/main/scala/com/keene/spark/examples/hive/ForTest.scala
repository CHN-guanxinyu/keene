package com.keene.spark.examples.hive

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.spark.utils.SimpleSpark

class ForTest extends Runner with SimpleSpark{
  override def run(implicit argv: Array[String]): Unit = {
    spark.fromKafka("kafka-broker2.jd.local:9092", "kafka-broker2.jd.local:9092").toConsole
    spark.streams.awaitAnyTermination
  }
}
