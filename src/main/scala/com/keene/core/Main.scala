package com.keene.core

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.core.implicits._

object Main extends App {
  def arg  = ArgumentsParser[MainArg](args)
  s"${arg.`class`}".as[Runner] run
    args ++ Seq(if( arg.man ) "--help" else "")
}

private[core] class MainArg(
  var `class` : String = "",
  var man : Boolean = false
) extends Arguments {

  def usage =
    """Options:
      |--class
      |--man        如果需要查看调用类的帮助,将此项设置为true,而不是--help
    """.stripMargin

}