/**
  * 这里封装了一些自定义的隐式转换
  * 可以方便的进行链式函数调用
  * 相比于`fun`(obj)的代码风格我更喜欢`str`.`fun`
  *
  */
package com.keene.core.implicits

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Implicitor {
  implicit def to(sql: String): StringImplicitor = StringImplicitor(sql)
  implicit def to(spark : SparkSession) : SparkSessionImplicitor = SparkSessionImplicitor(spark)
  implicit def to(df : DataFrame) : DataFrameImplicitor = DataFrameImplicitor(df)
}