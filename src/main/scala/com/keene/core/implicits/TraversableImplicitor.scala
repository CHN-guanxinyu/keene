package com.keene.core.implicits

import org.apache.spark.SparkContext

case class TraversableImplicitor[T](@transient t : T){
  def bc(implicit sc : SparkContext) : T = sc broadcast t value
}
