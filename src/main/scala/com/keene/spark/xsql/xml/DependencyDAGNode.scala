package com.keene.spark.xsql.xml

import com.keene.spark.xsql.xml.entity.Element

case class DependencyDAGNode (
  elem: Element,
  singleChild : Option[DependencyDAGNode],
  var parent: Option[Seq[DependencyDAGNode]]) {

  def print (deep: Int = 1): Unit = {
    println(List.fill(deep)("-+- ").mkString + elem)
    parent.foreach(_ foreach (_.print(deep + 1)))
  }

  override def hashCode () = elem.hashCode


  override def equals (obj: Any) = {
    obj match {
      case that : DependencyDAGNode => elem.equals(that.elem)
      case that : Element => elem.equals(that)
      case _ => false
    }
  }

}
