package com.keene.spark.xsql.base
import com.keene.core.implicits._
trait Factory[+T]

abstract class AbstractFactory[+T](suffix : String) extends Factory[T]{
  protected val packageTo : String

  def newInstance = (klass : String) =>  s"$packageTo.${klass.capitalize}$suffix".as[T]
}
