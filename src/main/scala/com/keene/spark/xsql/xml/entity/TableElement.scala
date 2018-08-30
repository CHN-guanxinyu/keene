package com.keene.spark.xsql.xml.entity

import scala.xml.Node

object TableElementFactory {
  def newInstance(tableNode : Node) = {
    tableNode \@ "source" match {
      case "" => CommonTableElement(tableNode)
      case source =>
        Class.forName(
          s"com.keene.spark.xsql.xml.entity.${
            source.split("-").map(_.capitalize).mkString
          }TableElement"
        ).getConstructor(classOf[Node]).
          newInstance(tableNode).
          asInstanceOf[TableElement]
    }
  }
}
/**
  * 中间节点,
  * from: required
  * sql : required
  * union-table : optional
  */
case class CommonTableElement(node : Node) extends TableElement
  with FromSupport
  with SqlSupport
  with UnionTableSupport
  with ConverterSupport


case class HiveTableElement(node : Node) extends TableElement
  with SqlSupport


case class HDFSTableElement(node : Node) extends TableElement
  with PathSupport
  with FormatSupport


case class TextFileTableElement(node : Node) extends TableElement
  with PathSupport
  with SplitSupport
  with SchemaSupport
