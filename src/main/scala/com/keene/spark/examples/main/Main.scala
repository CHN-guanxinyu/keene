package com.keene.spark.examples.main

import com.keene.core.parsers.{Arguments, ArgumentsParser => Parser}
import com.keene.core.implicits._

object Main extends App {
  val arg = Parser[MainArg](args)
  s"com.keene.spark.examples.${arg.klass}".as[ExampleRunner] run args
}

class MainArg(
  var klass : String = ""
) extends Arguments {

  def usage =
    """Options:
      |--klass
    """.stripMargin

}