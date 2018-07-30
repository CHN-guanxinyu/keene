package com.keene.core.parsers

import scala.reflect.ClassTag

/**
  * 参数解析器默认实现
  */
object ArgumentsParser{
  def apply[T](args : Array[String] , typ : String = "kv" , usage : String = "")(implicit tag: ClassTag[T]): ArgumentsParser =
    typ match {
      case "kv" => KValueTypeArgumentsParser[T](args , usage )
      case _ => throw new IllegalArgumentException(s"parser not found:$typ")
    }
}
trait ArgumentsParser extends Parser[Arguments]

trait Arguments