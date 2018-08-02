package com.keene.core.implicits

import org.apache.spark.SparkContext

case class AnyImplicitor[T](@transient t : T){
  def bc(implicit sc : SparkContext) : T = sc broadcast[T] t value
}
