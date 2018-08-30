package com.keene.spark.xsql.xml.entity

sealed trait Element{
  override def toString = s"[${this.getClass.getSimpleName}] => "
}

/**
  * 最终节点,出现在results文件中的节点
  * 所有的result节点都有from属性,FromSupport
  */
trait ResultElement extends Element
  with FromSupport

/**
  * 除了最终节点的其他节点,就是data-sources文件中的节点
  * 所有的table节点都有一个唯一标识自己的name属性,NameSupport
  */
trait TableElement extends Element
  with NameSupport
  with NullFillSupport{
  override def hashCode () = name.hashCode
  override def equals (obj: scala.Any) = name.equals(obj)
}
