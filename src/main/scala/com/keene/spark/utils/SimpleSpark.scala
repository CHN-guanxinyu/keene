package com.keene.spark.utils
import org.apache.spark.{SparkContext => Sc}
import org.slf4j.LoggerFactory
trait SimpleSpark[Support] extends BaseEnv {
  private final lazy val sparkPkg = "org.apache.spark"
  //core
  implicit final lazy val sc = Sc getOrCreate sparkConf

  //sql
  implicit final lazy val spark = {
    val spark = Class forName s"$sparkPkg.sql.SparkSession"
    var builder = spark getMethod "builder" invoke spark
    val builderClass = builder.getClass
    builder =
      if (isWindows) builder
      else builderClass.getMethod("enableHiveSupport") invoke builder

    builder =
      builderClass.getMethod("config", sparkConf.getClass).invoke(builder, sparkConf)

    builderClass.getMethod("getOrCreate").invoke(builder).asInstanceOf[Support]
  }

  lazy val logger = LoggerFactory getLogger "Console"


}