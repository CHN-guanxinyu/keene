package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext

import scala.util.Try

class NoReferWhiteList() extends AbstractVariable {

  var unions: Set[Long] = Set[Long]()

  override def init(sc: SparkContext, argv: ExecutorArgs): Unit = {
    val path = argv.inputNoreferWhitelist
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day:String) : Unit = {
    unions = sc.textFile(path).map { line =>
      val fields = line.split("\t", -1)
      if (fields.length == 5 && Try(fields(1).toLong).isSuccess) {
        //val isThirdParty = fields(0)
        val unionid = fields(1)
        //val subunionid = fields(2)
        val start_time = fields(3)
        val end_time = fields(4)
        if (isKeyValid(day, start_time, end_time)==true) {
          unionid.toLong
        } else {
          0L
        }
      } else {
        0L
      }
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
