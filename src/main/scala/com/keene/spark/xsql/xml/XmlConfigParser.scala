package com.keene.spark.xsql.xml

import com.keene.core.parsers.Parser
import com.keene.spark.xsql.xml.entity._

import scala.collection.mutable
import scala.xml.{Node, XML}

/**
  * 这是一个字符串到二元组(Seq[ DependencyDAGNode ], Map[String, Seq[String] ])的解析器
  * configPath -> (依赖森林:results标签为根, 待解析的column-group的Map)
  */
object XmlConfigParser extends Parser[String, (Seq[DependencyDAGNode], Map[String, Seq[String]])]{

  /**
    * 计算框架配置解析器主入口
    *
    * @param path
    * @return
    */
  override def parse(path: String) = {
    val resultsNode = XML.loadFile(path)

    //准备阶段,从每个result节点开始,逆向解析,加载所有节点信息,为后续操作做准备
    resolveDependencies(resultsNode)

    //返回(依赖图,外部字段mapper)元组
    (genDAG(resultsNode), _groupIdColumnsMapper.toMap)
  }
  /**
    * 加载当前results节点的所有依赖节点
    *
    * @param node `<results>`标签节点
    */
  private def resolveDependencies (node: Node) {

    //加载所有dataSources
    node \\ "dependency" map (_ \@ "file") foreach parseDataSources

    //查重,重复的表名将引发错误
    checkDup(_namedTableNodes.map(_._1), "表名")

    //第一次转换
    //将`表名->xml.Node`映射转换为`表名->Element`映射
    _tableElemMapper ++= _namedTableNodes.toMap.mapValues(entityOf)
  }

  /**
    * 解析`<data-sources>`标签中的所有`<table>`标签
    * 并将结果写入`_namedTableNodes`
    * 同时递归解析其依赖文件
    *
    * @param path
    */
  private def parseDataSources (path: String): Unit = {
    _filesThoseAlreadyParsed += path
    val root = XML.loadFile(path)

    //建立索引
    _namedTableNodes ++= root \ "table" map { table => table \@ "name" -> table }

    //解析外部字段
    parseColumnGroups(root)

    //递归解析父级文件
    val dependencies = root \\ "dependency"
    if (dependencies.nonEmpty)
      dependencies.map(_ \@ "file").
        filterNot(_filesThoseAlreadyParsed.contains).
        foreach(parseDataSources)
  }
  /**
    * 解析生成DAG
    *
    * @param node
    * @return
    */
  private def genDAG (node: Node) =
    //最底层的result节点转换为Element,再进一步转换为DAGNode
    node \ "result" map (genResultElement andThen toDependencyDAGNode(None))

  private def genResultElement: Node => ResultElement = ResultElementFactory.newInstance

  /**
    * 自定义xml节点实体转换为最终结果DAG的节点类型
    * @param child 最后需要根据此节点进行cache策略生成
    * @return
    */
  private def toDependencyDAGNode(child : Option[DependencyDAGNode]) : Element => DependencyDAGNode = elem => {
    val node = DependencyDAGNode(elem, child, None)
    node.parent = parentsOf(node)
    node
  }


  private def checkDup[ T ] (
    list: Seq[ T ],
    msg: String
  ) {
    val t = list diff list.toSet.toSeq
    require(t.isEmpty,
      s"""
         |[检查配置文件]
         |重复的$msg:${t.mkString(";")}
       """.stripMargin)
  }



  /**
    * 解析当前文件的`<column-group>`
    * @param root
    * @return
    */
  private def parseColumnGroups(root : Node) ={
    val ec = extColumns(root)

    val exc = _groupIdColumnsMapper.keySet & ec.keySet
    require(exc.isEmpty, s"跨文件存在相同group-id:${exc.mkString(";")}")

    _groupIdColumnsMapper ++= ec
  }

  /**
    *
    * @param root
    * @return groupId -> 组内最终的所有字段
    */
  private def extColumns (root: Node) = {
    val groups = (root \\ "column-group").view

    val ids = groups.map(_ \@ "id")
    checkDup(ids, "column-group id")

    val idColumns = ids.zip(groups.map(_ \ "column" map (_ \@ "name")).map(_.to[ mutable.ListBuffer ])) toMap
    val idIncludes = ids.zip(groups.map(_ \ "include" map (_ \@ "id"))).filter(_._2.nonEmpty) toMap

    //这里保证了可以多重include
    for((id, includeIds) <- idIncludes; includeId <- includeIds)
      idColumns(id) ++= idColumns(includeId)

    idColumns.mapValues(_.toSet.toSeq)
  }

  /**
    * 当前节点的父节点
    *
    * @param node
    * @return
    */
  private def parentsOf (node: DependencyDAGNode) = node.elem match {
    //FromSupport说明是第二层开始的节点,因为最上层的hive、hdfs等节点不需要from
    case elem: FromSupport => elem.from match {
      case "" => None
      case from =>
        //这里保证了可以依赖多表,Some里存放当前节点`node`的所有父节点
        Some(
          from.split(",").view.
            map(_.trim).
            map(_tableElemMapper).
            //找到父节点Element后,进行第二次转换到面向解析的DagNode
            map(toDependencyDAGNode(child = Some(node)))
        )
    }
    case _ => None
  }


  /**
    * 将xml节点类型转换为自定义xml节点实体
    *
    * @return
    */
  private def entityOf: Node => Element = node => node.label match {
    case "result" => genResultElement(node)
    case "table" => genTableElement(node)
  }

  private def genTableElement : Node => TableElement = TableElementFactory.newInstance

  private val _filesThoseAlreadyParsed = mutable.ListBuffer[String]()
  private val _namedTableNodes = mutable.ListBuffer[ (String, Node) ]()
  private val _tableElemMapper = mutable.Map[ String, Element ]()
  private val _groupIdColumnsMapper = mutable.Map[ String, Seq[String] ]()
}
