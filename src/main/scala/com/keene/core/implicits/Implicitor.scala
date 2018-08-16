/**
  * 这里封装了一些自定义的隐式转换
  * 可以方便的进行链式函数调用
  * 相比于`fun`(obj)的代码风格我更喜欢`str`.`fun`
  *
  */
package com.keene.core.implicits

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

trait Implicitor {
  implicit def to(sql: String) = StringImplicitor(sql)
  implicit def to(spark : SparkSession)  = SparkSessionImplicitor(spark)
  implicit def to(df : DataFrame) = DataFrameImplicitor(df)
  implicit def to[T](seq : Traversable[T]) = TraversableImlicitor[T](seq)
  implicit def to[T](t: T)(implicit tag : ClassTag[T]) = AnyImplicitor(t)
}