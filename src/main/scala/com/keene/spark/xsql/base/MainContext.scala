package com.keene.spark.xsql.base

import com.keene.core.implicits._
import com.keene.spark.xsql.MainArg
import com.keene.spark.xsql.io.DataManagerFactroy
import com.keene.spark.xsql.udaf.MapAgg
import com.keene.spark.xsql.xml.entity._
import com.keene.spark.xsql.xml.{DependencyDAGNode, XmlConfigParser}


class MainContext(arg : MainArg) extends ExecutionContext {
  def showDAG = _tableDag.foreach(_.print())
  def showCacheList =
    println(
      s"""
        |[cache-list]:
        | ${_toBeCached.sorted.mkString(";\n\t")}
      """.stripMargin
    )

  //对外唯一接口,开始运行计算任务
  def execute(debug : Boolean = false) = {
    if(debug) {
      showDAG
      showCacheList
    }
    _tableDag.par.foreach(run(debug))
  }


  private var _tableDag : Seq[DependencyDAGNode] = _
  private var _toBeCached : Seq[String] = _


  //解析xml配置,生成运行时依赖信息,生成字段组信息
  private val (tableDag, extColsMapper) = XmlConfigParser.parse(arg.configPath)
  _tableDag = tableDag

  //扫描依赖信息,生成缓存策略
  _toBeCached = flatten(tableDag.flatMap(_.parent).flatten).
    groupBy(_.elem).
    filter(_._2.length > 1).
    map(_._1.asInstanceOf[TableElement].name).toSeq

  //将字段组和脚本变量注册进上下文,为解析el表达式做准备
  setAll(extColsMapper.map{case (groupId, cols) => (s"COLUMN_GROUP.$groupId", cols.mkString(",")) })

  //注册udf等
  extEnv

  //加载外部自定义配置,注册用户自定义udf,udaf等
  private val extClass = arg.extClass
  if(extClass.nonEmpty)
    extClass.removeBlanks.split(",").foreach(_.as[ExecutionContext].extEnv)

  //扁平化所有节点
  private def flatten(nodes: Seq[DependencyDAGNode]): Seq[DependencyDAGNode] =
    nodes.flatMap(node => flatten(node.parent.getOrElse(Nil))) ++ nodes

  private def run(debug : Boolean)(dag : DependencyDAGNode) : Unit ={
    //dag.parent 是一个Option, dag.parent.foreach 就是循环单次,不需要判断是否是None了,
    dag.parent.foreach(_ foreach run(debug))

    dag.elem match {
      case elem : TableElement => register(elem)
      case elem : ConsoleResultElement if !debug => elem.from.tab.show(elem.show.toInt, false)
      case elem : ResultElement if !debug =>  DataManagerFactroy.newInstance(elem) store elem
      case _ =>
    }
  }

  /**
    * 这个函数比较重要
    * 是整个上下文表注册的逻辑
    * 根据不同的节点类型进行不同的注册策略
    * @param tableElem
    */
  private def register(tableElem: TableElement) : Unit = {
    val name = tableElem.name
    val nullFill = tableElem.nullFill
    val needCache = _toBeCached.contains(name)
    tableElem match{
      case elem : CommonTableElement =>
        //TODO 有待改进
        val from = elem.from
        var converterArg = from
        val ut = elem.unionTable
        val cv = elem.converter
        if(ut.nonEmpty){
          converterArg = ut
          from.split(",").
            map(_.tab).
            reduce(_ union _).
            createOrReplaceTempView(ut)
        }
        if(cv.nonEmpty)
          planWithCache(
            cv.as[Converter].convert(converterArg),
            name,
            needCache
          )
        else register(elem, needCache)
      case elem : SqlSupport => register(elem, needCache)
      case elem => DataManagerFactroy.newInstance(elem).load(elem, needCache)
    }
  }
  private def register(tableElem: TableElement with SqlSupport, cache : Boolean = false) : Unit ={
    val sqlStr = fixEl(tableElem.sql).replaceBlanks
    planWithCache(sqlStr, tableElem, cache, e =>
      s"""
      |[error]:
      |
      |$e
      |
      |发生在:
      |<table name="${tableElem.name}"/>
      |
      |请检查解析后sql:
      |$sqlStr
      """.stripMargin
    )
  }

  private def fixEl(sql : String) ={
    val elRegex = "\\$\\{(.*?)\\}".r
    (sql /: elRegex.findAllMatchIn(sql)){ case (fixedSql, matcher) =>
      val k = matcher.group(1)
      val replacement = getVar(k)
      val findBy = "\\$\\{" + k + "\\}"
      fixedSql.replaceAll(findBy, replacement)
    }
  }


  override def extEnv: Unit ={
    spark.udf.register("map_agg", new MapAgg)
  }
}
