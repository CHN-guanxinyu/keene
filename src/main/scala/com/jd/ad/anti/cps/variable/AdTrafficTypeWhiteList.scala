package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext

import scala.util.Try

class AdTrafficTypeWhiteList() extends AbstractVariable {

  var allSites: Map[Long, List[String]] = Map()
  
  override def init(sc: SparkContext,argv: ExecutorArgs): Unit = {
    val path = argv.inputAdtWhitelist
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day: String) : Unit = {
    allSites = sc.textFile(path).map { line =>
      val fields = line.split("\t",-1)
      if (fields.length == 4 && Try(fields(0).toLong).isSuccess) {
        val adt = fields(0)
        val url = fields(1)
        val start_time = fields(2)
        val end_time = fields(3)
        if (isKeyValid(day, start_time, end_time)==true) {
          ( adt.toLong,
            url.replaceFirst("^http[s]{0,1}://(www.)?",
              "").replaceFirst("/.*$", "").toLowerCase)
        } else {
          (0L, "")
        }
      } else {
        (0L, "")
      }
    }.groupByKey().map { case (key, values) =>
      (key, values.toList)
    }.collectAsMap.toMap
  }

  override def contains(args: List[String]): Boolean = {
    var find = false
    if (args.length == 2) {
      val unionId = args(0).toLong
      val refer = args(1)
      val sites = allSites.getOrElse(unionId, List())
      for (site <- sites) {
        if (refer.contains(site))
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
