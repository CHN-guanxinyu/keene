package com.jd.ads_anti

import com.keene.core.Runner
import com.keene.core.parsers.Arguments
import com.keene.core.parsers.{ArgumentsParser => Parser}
import com.keene.core.implicits._

object Main extends App {
  val arg = Parser[MainArgs](args)

  s"com.keene.spark.examples.${arg.`class`}".as[Runner] run
    args ++ Seq(if( arg.man ) "--help" else "")
}

class MainArgs (
  var `class`: String = "",
  var man: Boolean = false
) extends Arguments {
  override def usage =
    """Options:
      |--class      在com.jd.ads_anti下的相对路径,eg:`foo.Bar` for `com.jd.ads_anti.foo.Bar`
      |--man        如果需要查看调用类的帮助,将此项设置为true,而不是--help
    """.stripMargin
}

