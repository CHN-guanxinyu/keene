package com.keene.spark.examples.main

import com.keene.core.parsers.{Arguments, ArgumentsParser => Parser}
import com.keene.core.implicits._

object Main extends App {
  val arg = Parser[MainArg](args)
  s"com.keene.spark.examples.${arg.klass}".as[ExampleRunner] run args ++ Seq(if( arg.man ) "--help" else "")
}

class MainArg(
  var klass : String = "",
  var man : Boolean = false
) extends Arguments {

  def usage =
    """Options:
      |--klass
      |--man        如果需要查看调用类的帮助,将此项设置为true,而不是--help
    """.stripMargin

}