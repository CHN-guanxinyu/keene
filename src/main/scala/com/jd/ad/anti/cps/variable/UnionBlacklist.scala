package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext

import scala.util.Try

/**
 *
 * Created by yfchengkai on 2017/9/7.
 */
class UnionBlacklist() extends AbstractVariable{

  var unions: Map[Long, List[String]] = Map()

  override def init(sc: SparkContext, argv: ExecutorArgs): Unit = {
    val path = argv.inputUnionBlacklist
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day: String) : Unit = {
    unions = sc.textFile(path).map {  line =>
      val fields = line.split("\t")
      //这块之所以是判断长度>=1是因为有的记录的格式为：111111\t,这样split后的长度是为1的.
      if (fields.length >= 1 && Try(fields(0).toLong).isSuccess) {
        (fields(0).toLong, getSubUnionId(fields))
      } else {
        (0L, "")
      }
    }.groupByKey().map { case (key, values) =>
      (key, values.toList)
    }.collectAsMap.toMap
  }

  /**
   * 需要对sub_union_id做特殊处理.
   * @param fields
   * @return
   */
  private def getSubUnionId(fields : Array[String]): String = {
    if(fields.length == 1 || fields(1) == null || "".equals(fields(1).trim())
        || "null".equals(fields(1).trim.toLowerCase()))
      "null"
    else
      fields(1).trim()
  }

  override def containKey(args: List[String]): Boolean = {
    var find = false
    val unionId = args(0).toLong
    if (unions.contains(unionId)) {
      find = true
    }
    find
  }

  override def contains(args: List[String]): Boolean = {
    var find = false
    if (args.length == 2) {
      val subUnionIds = unions.getOrElse(args(0).toLong, List())
      if (subUnionIds.contains(args(1).trim()))
        find = true
    }
    find
  }
}
