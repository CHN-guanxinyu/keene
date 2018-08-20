package com.keene.spark.examples.hive

import com.keene.core.Runner
import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark

class Free extends Runner with SimpleSpark {
  override def run (args: Array[ String ]): Unit = {
    val arg = ArgumentsParser[FreeArg](args)
  }
}

class FreeArg(
  var date : String = ""
) extends Arguments {
  override def usage =
    """Usage:
      |
      |--date
    """.stripMargin
}