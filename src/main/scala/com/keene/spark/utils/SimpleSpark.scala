package com.keene.spark.utils
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession => Sss}
import org.apache.spark.streaming.{Seconds, StreamingContext => Ssc}
import org.apache.spark.{SparkContext => Sc}

trait SimpleSpark extends BaseEnv {

  final lazy val sc = Sc getOrCreate sparkConf

  final lazy val spark = {
    val t = Sss.builder

    val builder = if (isWindows) t else t.enableHiveSupport

    builder config sparkConf getOrCreate
  }

  lazy val ssc: Ssc = Ssc.getActiveOrCreate(() =>
    new Ssc(sparkConf, Seconds(second))
  )

  final def jdbc(
                  url: String,
                  table: String,
                  user: String,
                  passwd: String
                ): DataFrame = {

    val prop = new Properties() >>
      (_ setProperty("user", user)) >>
      (_ setProperty("password", passwd))

    spark.read jdbc(url, table, prop)
  }

  implicit final class And[T](obj: T) {
    def >>(f: T => Unit): T = {
      f(obj);obj
    }
  }

}