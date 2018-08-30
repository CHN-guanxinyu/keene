package com.keene.spark.xsql.xml.entity

import scala.xml.Node

object ResultElementFactory {
  def newInstance(tableNode : Node) = {
    val to = tableNode \@ "to"
    Class.forName(
      s"com.keene.spark.xsql.xml.entity.${
        to.split("-").map(_.capitalize).mkString
      }ResultElement"
    ).getConstructor(classOf[Node]).
      newInstance(tableNode).
      asInstanceOf[ResultElement]
  }
}
case class ConsoleResultElement(node : Node) extends ResultElement
  with ShowSupport

case class HdfsResultElement(node : Node) extends ResultElement
  with PathSupport
  with RepartitionSupport
  with FormatSupport

case class HiveResultElement(node : Node) extends ResultElement
  with RepartitionSupport

case class TextFileResultElement(node : Node) extends ResultElement
  with PathSupport
  with SplitSupport
  with RepartitionSupport

