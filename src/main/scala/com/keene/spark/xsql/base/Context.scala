package com.keene.spark.xsql.base

import scala.collection.mutable
trait Context{
  private val _conf = mutable.Map[String, String]()

  final def setVar(k : String, v : String) = _conf += (k -> v)
  final def setAll(opts : Map[String, Any]) = _conf ++= opts.mapValues(_.toString)
  final def getVar(k : String, orElse : String = "") = _conf.getOrElse(k, orElse)
}
