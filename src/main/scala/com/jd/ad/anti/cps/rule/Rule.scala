package com.jd.ad.anti.cps.rule

import com.jd.ad.anti.cps.yaml._

abstract class Rule(conf: _Rule) {
  /*
   * 返回true代表击中规则
   */
  def run(row: Map[String,Any]): Boolean
}