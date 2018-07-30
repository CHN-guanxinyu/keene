package com.keene.core.parsers

import scala.reflect.ClassTag

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
  */
case class KValueTypeArgumentsParser[T]( usage: String = "" )(implicit t: ClassTag[T])
  extends ArgumentsParser[T] {

  private lazy val klass = t.runtimeClass

  /**
    * 解析器主入口
    * 解析流程
    * 1.拿到默认参数实体类
    * 2.将args拼成k-v对
    * 3.遍历kv对,对于每个k,找到相应setter,并将v set进去
    * @return
    */
  override def parse(args: Array[String]) : T = {
    stopIfNeedHelp(args)

    val resultObj = defaultInstance

    val argsList: List[String] = args toList
    val pair = argsList.grouped(2).map { case k :: v :: Nil => (k, v) }.toList

    pair.foreach { case (k, v) =>
      val w = k drop 2 split "-"
      val methodName = w.head + w.tail.map(_ capitalize).mkString

      val setter = namedSetterMapper(methodName)

      //处理类型问题
      //先定义几种方案,当以下方案都不可行则抛异常
      val plans = Iterator[String => Any](
        _ toInt ,
        _ toDouble ,
        _ toFloat ,
        _ toBoolean
      )

      def tryOrElse(plan: String => Any):Unit = {
        if( plan == null ) throw new IllegalArgumentException("cannot fix the type")

        try setter.invoke( resultObj, plan(v).asInstanceOf[Object] )
        catch { case _ =>
          tryOrElse( plans next() )
        }
      }

      tryOrElse( _.trim )
    }

    resultObj.asInstanceOf[T]
  }


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

  //按照给定`values`填充参数值
  private def newInstance(values: Array[Object]) = constructor.newInstance(values: _*)

  //获取默认参数实体类
  //步骤就是根据`apply$default$num`方法获取局部变量表中第`num`个默认值
  //再填充到已有对象中
  private def defaultInstance = newInstance( defaultValues )

  //参数实体类默认值列表
  private lazy val defaultValues: Array[AnyRef] = applyDefaultFun.map(_ invoke zero)

  //参数实体类的唯一构造器
  private lazy val constructor = klass.getConstructor(types: _*)

  //参数实体类构造方法参数类型列表
  private lazy val types: Array[Class[_]] =
    klass.
    getDeclaredConstructors.
    head.
    getParameterTypes

  //参数实体类成员变量名和对应setter的映射
  private lazy val namedSetterMapper =
    methods.filter(_.getName endsWith "_$eq").
    map(s => s.getName.replaceAll("_\\$eq", "") -> s).
    toMap



  //参数实体类由scala自动生成的获取默认参数值的方法
  private lazy val applyDefaultFun =
    methods.filter(_.getName startsWith "$lessinit").
    sortBy( _.getName.split("\\$").last toInt )

  //参数实体类的所有方法
  private lazy val methods = klass.getDeclaredMethods




  //如果`--help`出现就显示Usage并退出
  private def stopIfNeedHelp(org: Array[String]): Unit =
    if (org contains "--help") {
      System.err.println(usage)
      System exit 1
    }
}

