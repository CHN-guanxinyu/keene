package com.keene.core.implicits

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

case class AnyImplicitor[T](@transient t : T)(implicit tag : ClassTag[T]){
  def bc(implicit sc : SparkContext) = sc.broadcast(t).value
}
