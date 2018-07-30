package com.keene.spark.utils
import org.apache.spark.sql.{SparkSession => Sss}
import org.apache.spark.streaming.{Seconds, StreamingContext => Ssc}
import org.apache.spark.{SparkContext => Sc}

trait SimpleSpark extends BaseEnv {
  //core
  final lazy val sc = Sc getOrCreate sparkConf

  //sql
  final lazy val spark = {
    val t = Sss.builder
    val builder = if (isWindows) t else t.enableHiveSupport
    builder config sparkConf getOrCreate
  }

  //streaming
  lazy val ssc: Ssc = Ssc.getActiveOrCreate(() =>
    new Ssc(sparkConf, Seconds(second))
  )

}