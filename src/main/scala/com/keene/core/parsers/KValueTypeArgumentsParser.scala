package com.keene.core.parsers

import java.lang.reflect.Constructor



object KValueTypeArgumentsParser {

  /**
    * 支持键值对形式的参数解析
    * Demo:
    *
    * KValueTypeArgumentsParser(args, classOf[MyArgs]/MyArgs.getClass ,
    *   """
    *   |Usage : xxx [--int-arg xxx] ....
    *   """.stripMargin
    * )
    *
    * Attention : 下面参数中`var`与默认值为必填
    * 支持以下几种类型
    * class MyArgs(
    *   var intArg: Int = 0,
    *   var stringArg: String = "111",
    *   var doubleArg: Double = 0.5,
    *   var floatArg : Float = 0.2f,
    *   var booleanArg : Boolean = false
    * ) extends Arguments
    *
    *
    *
    * @param args
    * @param klass
    * @param usage
    * @return
    */
  def apply(args: Array[String], klass: Class[_], usage: String = ""): Arguments =
    new KValueTypeArgumentsParser(args, klass, usage).parse
}

private class KValueTypeArgumentsParser(args: Array[String], _class: Class[_], usage: String) extends ArgumentsParser {

  def parse: Arguments = {
    stopIfNeedHelp(args)

    val resultObj = defaultInstance

    val argsList: List[String] = args toList
    val pair = argsList.grouped(2).map { case k :: v :: Nil => (k, v) }.toList

    pair.foreach { case (k, v) =>
      val w = k drop 2 split "-"
      val methodName = w.head + w.tail.map(_ capitalize).mkString

      val setter = _constructor.param.namedSetterMapper(methodName)
      val plans = Iterator[String => Any](
        _ toInt ,
        _ toDouble ,
        _ toFloat ,
        _ toBoolean
      )

      def tryElse(plan: String => Any):Unit = {

        if( plan == null ) throw new IllegalArgumentException("cannot fix the type")

        try setter.invoke(resultObj, plan(v).asInstanceOf[Object])
        catch { case _ =>
          tryElse(plans.next())
        }
      }

      tryElse( _.trim )
    }

    resultObj.asInstanceOf[Arguments]
  }


  object _constructor {
    val get: Constructor[_] = _class.getConstructor(param.types: _*)


    object param {
      lazy val types: Array[Class[_]] =
        _class.
          getDeclaredConstructors.
          head.
          getParameterTypes

      lazy val namedSetterMapper =
        $setters.map(s => s.getName.replaceAll("_\\$eq", "") -> s).toMap

      lazy val defaultValues: Array[AnyRef] = $adf.map(_ invoke zero)

      lazy val $setters =
        $ms.filter(_.getName endsWith "_$eq")

      lazy val $adf =
        $ms.filter(_.getName startsWith "apply$").
          sortBy(_.getName.split("\\$").last.toInt)

      lazy val $ms = _class.getDeclaredMethods
    }

  }

  def newInstance(values: Array[Object]) = _constructor.get.newInstance(values: _*)

  def defaultInstance = {
    val applyDefaultsParams = _constructor.param.$adf
    val z = zero
    val defaultValues = applyDefaultsParams.map(_ invoke z)
    newInstance(defaultValues)
  }

  lazy val zero = {
    val values = _constructor.param.types.view.
      map(_.getSimpleName.toLowerCase).
      map {
        case "boolean" => false
        case "float" => 0f
        case "double" => 0.0
        case "int" => 0
        case "string" => ""
      }.
      map(_.asInstanceOf[Object]).toArray

    newInstance(values)
  }

  def stopIfNeedHelp(org: Array[String]) =
    if (org contains "--help") {
      System.err.println(usage)
      System exit 1
    }
}

