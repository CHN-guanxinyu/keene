package com.keene.core

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.core.implicits._

object Main extends App {

  val arg  = args.as[MainArg]._1

  implicit val argv = args ++ Array(if( arg man ) "--help" else "")

  arg.`class`.as[Runner].run

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