package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import scala.util.Try

class SocialRegisterSite() extends AbstractVariable {

  var allSites: Map[Long, List[String]] = Map()

  override def init(sc: SparkContext, argv: ExecutorArgs): Unit = {
    val path = argv.inputUnionWebSocial
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day: String) : Unit = {
    allSites = sc.textFile(path).map { line =>
      val fields = line.split("\t")
      if (fields.length == 2 && Try(fields(0).toLong).isSuccess)
        (fields(0).toLong,
          fields(1).replaceFirst("^http[s]{0,1}://(www.)?",
            "").replaceFirst("/.*$", "").toLowerCase)
      else
        (0L, "")
    }.groupByKey().map {
      case (key, values) =>
        (key, values.toList)
    }.collectAsMap.toMap
//    println("allSites count: " + allSites.size)
  }

  override def contains(args: List[String]): Boolean = {
    var find = false
    if (args.length == 2) {
      val unionId = args(0).toLong
      val host = args(1)
      val sites = allSites.getOrElse(unionId, List())
      for (keyword <- sites) {
        if (host.contains(keyword))
          find = true
      }
    }
    find
  }

  override def containKey(args: List[String]): Boolean = {
    var find = false
    val unionId = args(0).toLong
    if (allSites.contains(unionId)) {
      find = true
    }
    find
  }
}
