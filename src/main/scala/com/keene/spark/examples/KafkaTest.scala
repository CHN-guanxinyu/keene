package com.keene.spark.examples

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._
import com.keene.kafka.{KafkaParam, KafkaWriterParam}

object KafkaTest extends App with SimpleSpark {

  val arg = ArgumentsParser[KafkaReadArgs](args)

  val readParam = KafkaParam( arg.brokers , arg.subscribe )

  spark fromKafka readParam createOrReplaceTempView "t"

  implicit val writeParam = KafkaParam( arg.brokers , arg.topic , as = "writer")

  "select * from t".go.toKafka start

  "select base64(CAST(value as STRING)) value from t".go.toKafka start

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
