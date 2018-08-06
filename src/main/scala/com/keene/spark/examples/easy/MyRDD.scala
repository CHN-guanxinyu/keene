package com.keene.spark.examples.easy

import akka.actor._
import com.keene.spark.utils.SimpleSpark

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Success
object Main extends SimpleSpark {
  def main (args: Array[ String ]): Unit = {
    List(1,2).par.foreach(println)

    println(3)

  }
}
