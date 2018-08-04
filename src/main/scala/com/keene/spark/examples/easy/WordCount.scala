package com.keene.spark.examples.easy

import com.keene.core.Runner
import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark

object Test extends SimpleSpark with App {
  val a = Set(1)
  a & a
}
class WordCount extends Runner with SimpleSpark{
  override def run (args: Array[ String ]): Unit = {
    val argv = ArgumentsParser[WordCountArg](args)
    val lines = sc textFile argv.path
    val wordCount = lines.flatMap(_ split "\t").map(_ -> 1).reduceByKey( _+_ )
    wordCount.sortBy(_._2, false).collect.foreach(println)
  }
}

class WordCountArg(
  var path : String = ""
) extends Arguments {
  override def usage = ""
}
