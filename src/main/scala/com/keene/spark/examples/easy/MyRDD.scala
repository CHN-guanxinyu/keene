package com.keene.spark.examples.easy

import com.keene.spark.utils.SimpleSpark
object Main extends SimpleSpark {
  def main (args: Array[ String ]): Unit = {
    List(1,2).par.foreach(println)

    println(3)

  }
}
