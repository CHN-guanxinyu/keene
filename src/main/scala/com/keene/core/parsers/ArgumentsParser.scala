package com.keene.core.parsers

import scala.reflect.ClassTag

/**
  * 参数解析器默认实现
  */
object ArgumentsParser{
  def apply[T](args : Array[String] , typ : String = "kv" )(implicit tag: ClassTag[T]): T = {
    val parser = typ match {
      case "kv" => KValueTypeArgumentsParser[T]
      case _ => throw new IllegalArgumentException(s"parser not found:$typ")
    }

    parser parse args
  }

}
trait ArgumentsParser[T] extends Parser[Array[String] , T]

trait Arguments{
  def usage:String
}