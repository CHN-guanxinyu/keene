package com.jd.ad.anti.cps.rule

import com.jd.ad.anti.cps.yaml._
/**
  * Created by Jun on 2017/7/15.
  */
class SpaceRule(conf: _Rule) extends Rule(conf) {
  def run(row: Map[String,Any]): Boolean = {
    // Todo
    true
  }
}
