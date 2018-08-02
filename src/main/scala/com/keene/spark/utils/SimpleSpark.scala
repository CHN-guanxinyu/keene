package com.keene.spark.utils
import org.apache.logging.log4j.core.Logger
import org.apache.spark.sql.{SparkSession => Sss}
import org.apache.spark.streaming.{Seconds, StreamingContext => Ssc}
import org.apache.spark.{SparkContext => Sc}
import org.slf4j.LoggerFactory

trait SimpleSpark extends BaseEnv {
  //core
  implicit final lazy val sc = Sc getOrCreate sparkConf

  //sql
  implicit final lazy val spark = {
    val t = Sss.builder
    val builder = if (isWindows) t else t.enableHiveSupport
    builder config sparkConf getOrCreate
  }

  //streaming
  implicit final lazy val ssc: Ssc = Ssc.getActiveOrCreate(() =>
    new Ssc(sparkConf, Seconds(second))
  )

  lazy val logger = LoggerFactory getLogger "Console"

  def info = logger info( _: String)
  def debug = logger debug( _: String)
  def warn = logger warn( _: String)
  def error = logger error ( _: String)
}