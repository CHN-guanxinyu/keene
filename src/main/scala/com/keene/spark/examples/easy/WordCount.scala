package com.keene.spark.examples.easy

import com.keene.core.Runner
import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._

class WordCount extends Runner with SimpleSpark{
  override def run (implicit args: Array[ String ]): Unit = {
    val argv = args.as[WordCountArg]
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
