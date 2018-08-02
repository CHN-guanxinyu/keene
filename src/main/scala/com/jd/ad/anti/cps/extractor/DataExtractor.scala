package com.jd.ad.anti.cps.extractor

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import java.util.Date

import com.jd.ad.anti.cps.ExecutorArgs
import scala.util.Try

abstract class DataExtractor extends Serializable {
  def getRDD(sc: SparkContext, argv: ExecutorArgs): RDD[Map[String, Any]]
  
  def getStringOrElse(row: Row, name: String, value: String = "") : String = {
    val v = row.getAs[String](name)
    (if (v == null || v.toLowerCase == "null") value else v)
  }
  
  def getStringAsLong(row: Row, name: String, value: Long = 0L) : Long = {
    val v = row.getAs[String](name)
    (if ((v == null) || Try(v.toLong).isFailure) value else v.toLong)
  }

  def getStringAsInt(row: Row, name: String, value: Int = 0) : Int = {
    val v = row.getAs[String](name)
    (if ((v == null) || Try(v.toInt).isFailure) value else v.toInt)
  }

}
