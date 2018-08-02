package com.jd.ad.anti.cps.realtime

class LocalTimer extends Serializable  {
  var start = System.currentTimeMillis()

  def reset():Unit = {
    start = System.currentTimeMillis()
  }

  def cost():Long = {
    System.currentTimeMillis() - start
  }
}
