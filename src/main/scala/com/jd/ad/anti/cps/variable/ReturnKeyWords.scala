package com.jd.ad.anti.cps.variable

import com.jd.ad.anti.cps.ExecutorArgs
import org.apache.spark.SparkContext

class ReturnKeyWords() extends AbstractVariable {

  var allSites: Map[String,  String ] = Map()
  
  override def init(sc: SparkContext,argv: ExecutorArgs): Unit = {
    val path = argv.inputReturnKeyWords
    updateData(sc, path, getRunDay(argv))
  }

  override def updateData(sc: SparkContext, path: String, day: String) : Unit = {
    allSites=sc.textFile(path).map { line =>
      val fields = line.split("\t",-1)
      if (fields.length == 4 ) {
        val keyword_type = fields(0).trim
        val keyword = fields(1).trim
        val start_time = fields(2).trim
        val end_time = fields(3).trim
        if (isKeyValid(day, start_time, end_time)==true) {
              (keyword_type -> keyword)
          } else {
            ("null" -> "")
          }
        }
        else {
          ("null" -> "")
      }
    }.reduceByKey((keyword_1 , keyword_2)=>{
        keyword_1+"|"+keyword_2
      }).collectAsMap.toMap
  }

  override def contains(args: List[String]): Boolean = {
    var find = true
    find
  }

  override def containKey(args: List[String]): Boolean = {
    var find = false
    val blackLike = allSites.getOrElse("black_like", "").r
    val blackNotLike = allSites.getOrElse("black_not_like", "").r
    val whiteLike = allSites.getOrElse("white_like", "").r
    val whiteNotLike = allSites.getOrElse("white_not_like", "").r

    val infoList = args(0).replaceAll("[List\\(\\)]","")
            .split(",").map(_.trim).toList
    for (info <- infoList) {
      if (0 != blackLike.findAllIn(info).length && 0 == blackNotLike.findAllIn(info).length )
         find = true
    }
    for (info <- infoList) {
      if (0 != whiteLike.findAllIn(info).length && 0 == whiteNotLike.findAllIn(info).length )
         find = false
    }
    find
  }
}
