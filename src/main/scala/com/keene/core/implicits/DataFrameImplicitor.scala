package com.keene.core.implicits

import com.keene.kafka.KafkaParam
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

case class DataFrameImplicitor(@transient df : DataFrame) {
  def toKafka(implicit kafkaParam: KafkaParam): DataStreamWriter[Row] =
    df.writeStream.
    options( kafkaParam.get ).
    format("kafka")
}