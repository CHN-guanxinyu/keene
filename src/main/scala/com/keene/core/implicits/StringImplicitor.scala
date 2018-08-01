package com.keene.core.implicits

import org.apache.spark.sql.{DataFrame, SparkSession}


case class StringImplicitor(@transient str : String) {
  lazy val spark = SparkSession.builder.enableHiveSupport.getOrCreate

  /**
    * Usage: "package.to.classA".as[classA].foo.bar
    *
    * @tparam T
    * @return
    */
  def as[T] = Class.forName( str ).getConstructor().newInstance().asInstanceOf[T]

  /**
    * Usage: "select something".go where "cond1" show false
    * @return
    */
  def go: DataFrame = spark sql str
}

