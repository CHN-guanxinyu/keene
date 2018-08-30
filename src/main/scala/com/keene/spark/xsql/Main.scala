package com.keene.spark.xsql

import com.keene.core.parsers.Arguments
import com.keene.spark.xsql.base.MainContext
import com.keene.core.implicits._

object Main {
  def main (args: Array[ String ]): Unit = {
    val (arg, varMap) = args.as[MainArg]
    val ctx = new MainContext(arg)
    ctx.setAll(varMap)
    ctx.execute(arg.debug)
  }
}


class MainArg(
  var configPath : String = "",
  var extClass : String = "",
  var debug : Boolean = false
) extends Arguments {
  def usage =
    """Options:
      |
      |--config-path
      |--ext-class
      |--debug
    """.stripMargin

}

