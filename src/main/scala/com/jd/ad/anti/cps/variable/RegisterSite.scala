package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext
import scala.util.Try

class RegisterSite () extends AbstractVariable {

  var allSites: Map[Long, List[String]] = Map()
  
  override def init(sc: SparkContext,argv: ExecutorArgs): Unit = {
    val path = argv.inputUnionWeb
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
