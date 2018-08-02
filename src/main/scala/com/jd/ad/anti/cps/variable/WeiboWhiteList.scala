package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import scala.util.Try

class WeiboWhiteList() extends AbstractVariable {

  var unions: Set[Long] = Set[Long]()

  override def init(sc: SparkContext, argv: ExecutorArgs): Unit = {
    val path = argv.inputWeiboWhitelist
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day: String) : Unit = {
    unions = sc.textFile(path).map { line =>
      if (Try(line.toLong).isSuccess) line.toLong else 0L
    }.collect.toSet
  }

  override def contains(args: List[String]): Boolean = {
    var find = false
    if (args.length == 1 && Try(args(0).toLong).isSuccess) {
      val unionId = args(0).toLong
      find = unions.contains(unionId)
    }
    find
  }
  
  override def containKey(args: List[String]): Boolean = {
    var find = false
    val unionId = args(0).toLong
    if (unions.contains(unionId)) {
      find = true
    }
    find
  }
}
