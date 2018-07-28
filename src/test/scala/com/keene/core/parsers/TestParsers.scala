package com.keene.core.parsers

object TestArgsParser extends App {

  println( KValueTypeArgumentsParser(args, classOf[MyArgs] ,
    """
      |Usage : xxx [--int-arg xxx] ....
    """.stripMargin

  ))
}

case class MyArgs(
                   var intArg: Int = 0,
                   var stringArg: String = "111",
                   var doubleArg: Double = 0.5,
                   var floatArg : Float = 0.2f,
                   var booleanArg : Boolean = false
                 ) extends Arguments