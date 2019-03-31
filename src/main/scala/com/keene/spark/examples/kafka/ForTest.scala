package com.keene.spark.examples.kafka

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.spark.utils.SimpleSpark
import org.apache.spark.sql.SparkSession

class ForTest extends Runner with SimpleSpark[SparkSession]{
  override def run(implicit argv: Array[String]): Unit = {
    spark.fromKafka("kafka-broker2.jd.local:9092", "dx_anti_search_click_logs").toConsole
    spark.streams.awaitAnyTermination()
  }
}
