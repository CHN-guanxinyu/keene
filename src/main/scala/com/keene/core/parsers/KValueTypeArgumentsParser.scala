package com.keene.core.parsers



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

  /**
    * 解析器主入口
    * @return
    */
  def parse: Arguments = {
    stopIfNeedHelp(args)

    val resultObj = defaultInstance

    val argsList: List[String] = args toList
    val pair = argsList.grouped(2).map { case k :: v :: Nil => (k, v) }.toList

    pair.foreach { case (k, v) =>
      val w = k drop 2 split "-"
      val methodName = w.head + w.tail.map(_ capitalize).mkString

      val setter = namedSetterMapper(methodName)
      val plans = Iterator[String => Any](
        _ toInt ,
        _ toDouble ,
        _ toFloat ,
        _ toBoolean
      )

      def tryElse(plan: String => Any):Unit = {

        if( plan == null ) throw new IllegalArgumentException("cannot fix the type")

        try setter.invoke(resultObj, plan(v).asInstanceOf[Object])
        catch { case _: Throwable =>
          tryElse(plans.next())
        }
      }

      tryElse( _.trim )
    }

    resultObj.asInstanceOf[Arguments]
  }


  //按照给定`values`设置参数值
  def newInstance(values: Array[Object]) = constructor.newInstance(values: _*)

  //获取默认参数实体类
  def defaultInstance = {
    val applyDefaultsParams = $adf
    val z = zero
    val defaultValues = applyDefaultsParams.map(_ invoke z)
    newInstance(defaultValues)
  }

  //参数实体类的唯一构造器
  lazy val constructor = _class.getConstructor(types: _*)

  //参数实体类构造方法参数类型列表
  lazy val types: Array[Class[_]] =
    _class.
    getDeclaredConstructors.
    head.
    getParameterTypes

  //参数实体类成员变量名和对应setter的映射
  lazy val namedSetterMapper =
    $setters.map(s => s.getName.replaceAll("_\\$eq", "") -> s).toMap

  //参数实体类默认值列表
  lazy val defaultValues: Array[AnyRef] = $adf.map(_ invoke zero)

  //参数实体类由scala自动生成的setter方法
  lazy val $setters =
    $ms.filter(_.getName endsWith "_$eq")

  //参数实体类由scala自动生成的获取默认参数值的方法
  lazy val $adf =
    $ms.filter(_.getName startsWith "apply$").
      sortBy(_.getName.split("\\$").last.toInt)

  //参数实体类的所有方法
  lazy val $ms = _class.getDeclaredMethods

  //参数实体类零元
  lazy val zero = {
    val values = types.view.
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


  //如果`--help`出现就显示Usage并退出
  def stopIfNeedHelp(org: Array[String]) =
    if (org contains "--help") {
      System.err.println(usage)
      System exit 1
    }
}

