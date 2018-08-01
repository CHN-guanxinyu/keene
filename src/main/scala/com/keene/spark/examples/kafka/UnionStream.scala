package com.keene.spark.examples.kafka

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.kafka.KafkaParam
import com.keene.spark.utils.SimpleSpark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
/**
  * 多流聚合
  * 其实跟是否是Kafka无关了
  * Streaming的范围
  */
class UnionStream extends SimpleSpark with Runner{
  override def run(args: Array[String]): Unit = {
    import spark.implicits._

    val arg = ArgumentsParser[Lv2Args](args)

    val readParam = KafkaParam(arg.brokers , arg.subscribe)
    val writeParam = KafkaParam(arg.brokers , arg.topic , "writer")

    val schm = StructType(Seq(
      StructField("key" , StringType ),
      StructField("value" , StringType)
    ))

    val file = spark.read schema schm json arg.path select($"key" , $"value" as "v2")

    val kafka = spark.fromKafka(readParam).
      selectExpr( "CAST(value as string)" ).
      select( from_json($"value" , schm) as "json" ).
      select("json.*").select($"key" , $"value" as "v1")

    val unionDS = file.join(kafka , Seq("key")).
      selectExpr("key","concat_ws('' , v1 , v2) as value")

    unionDS.toKafka(writeParam).start

    spark.streams.awaitAnyTermination
  }

  override def sparkConfOpts = super.sparkConfOpts ++ Map(
    "spark.sql.streaming.checkpointLocation" -> "E:/tmp/spark"
  )
}

class Lv2Args(
  var brokers : String = "",
  var subscribe : String = "",
  var topic : String = "",
  var path : String = ""
) extends Arguments {
  override def usage =
    """
      |--brokers
      |--subscribe
      |--topic
      |--path                 json file path
    """.stripMargin
}

