package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

class AddressList() extends AbstractVariable {

  var allSites: Set[String] = Set[String]()
  
  override def init(sc: SparkContext,argv: ExecutorArgs): Unit = {
    val path = argv.inputAddressSite
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day: String) : Unit = {
    allSites = sc.textFile(path).map { line =>
      if(line == null || "".equals(line.trim)) {
        "null"
      }else {
        val fields = line.split("\t", -1)
        if (fields.length == 3) {
          val address = fields(0)
          val start_time = fields(1)
          val end_time = fields(2)
          if (isKeyValid(day, start_time, end_time)==true) {
            address.trim
          } else {
            "null"
          }
        } else {
          "null"
        }
      }
    }.collect.toSet

  }


  override def contains(args: List[String]): Boolean = {
    var find = true
    find
  }

  override def containKey(args: List[String]): Boolean = {
    val infoList = args(0).split("\\|\\|").map(x => extractSite(x)).filter(_.nonEmpty)
    var previousCity: String = ""
    val newInfoList = new ListBuffer[String]()
    for (info <- infoList) {
      if (info != previousCity) {
        previousCity = info
        newInfoList.append(info)
      }
    }
    newInfoList.size > 3 && newInfoList.head == newInfoList.last
  }

  def extractSite(info: String): String = {
    var idx = info.length
    var target:String = ""
    for (site <- allSites){
      val pos = info.indexOf(site)
      if (pos >= 0 && pos < idx){
        target = site
        idx = pos
      }
    }
    target
  }
}
