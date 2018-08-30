package com.keene.spark.xsql.xml.entity


import scala.xml.Node
/**
  * 这里是xml节点所有可能的属性
  * 融入特质XxxSupport的方式, 便于之后的模式匹配操作
  *
  * 每当需要添加新的属性,需要在此处注册
  */
sealed trait AttributeSupport{
  val node : Node
  protected def attr( attrName : String, default : String = "") = {
    val v = node \@ attrName
    if(v.isEmpty) default else v
  }
}

/**
  * table节点属性 `union-table`
  *
  * union from中的表,unoin后的表名是union-table的值
  * 在后续sql中可以直接用这个union后的表
  */
trait UnionTableSupport extends AttributeSupport{
  val unionTable : String = attr("union-table")
}

/**
  * table节点属性 `sql`
  * 每一个表对应的sql语句
  */
trait SqlSupport extends AttributeSupport{
  val sql : String = attr("sql")
}

/**
  * table节点属性 `name`
  * 上下文中的表名
  * 确保他是唯一的
  */
trait NameSupport extends AttributeSupport{
  val name : String = attr("name")
  override def toString = super.toString + s" name : $name, "

}

/**
  * 结果节点属性 `show`
  * to="console"时,可进行show操作,默认20
  */
trait ShowSupport extends AttributeSupport{
  val show : String = attr("show", "20")
}

/**
  * table_withAttr_source/结果节点属性 `path`
  * 输入/输出路径
  */
trait PathSupport extends AttributeSupport{
  val path : String = attr("path")
}

/**
  * table节点属性 `from`
  * 代表当前sql语句用到哪些外部表
  * 多表用`,`分隔
  * 如果需要union,可以使用`union-table`属性
  */
trait FromSupport extends AttributeSupport{
  val from = attr("from")
  override def toString = super.toString + s" from : $from"

}

/**
  * 结果节点属性 `repartition-num`
  */
trait RepartitionSupport extends AttributeSupport{
  val repartitionNum = attr("repartition-num", "1000")
}

/**
  * 结果节点属性 `format`
  * 对应于spark sql 的format
  */
trait FormatSupport extends AttributeSupport{
  val format = attr("format", "orc")
}

/**
  * table_withAttr_source/结果节点属性 `split`
  * 根据split去读/写textFile
  */
trait SplitSupport extends AttributeSupport{
  val split = attr("split", "\t")
}

/**
  * textFile的table节点属性 `schema`
  * 切分后,以什么样的schema生成表
  */
trait SchemaSupport extends AttributeSupport{
  val schema = attr("schema")
}

/**
  * table节点的属性 `converter`
  * 有些复杂的逻辑使用sql无法实现
  * 需要借助spark代码实现
  * 这里通过converter属性指定AbcdConverter全类名,自定义的ABCDConverter继承Converter接口
  * 需要实现converter : String => DataFrame函数,传入的就是标签中from的所有的表名(按先后顺序,代码中可以直接使用,上下文已经有了)
  * 输出的就是想要的结果
  */
trait ConverterSupport extends AttributeSupport{
  val converter = attr("converter")
}

/**
  * 空值填充
  */
trait NullFillSupport extends AttributeSupport{
  val nullFill = attr("null-fill")
}