package com.jd.ad.anti.cps.filter

import com.jd.ad.anti.cps.yaml._Filter

/**
  * Created by Jun on 2017/7/15.
  */
object FilterFactory {
  def getFilter(conf: _Filter): Filter = {
    var filter:Filter = null
    if (conf.filter_type.equals("RuleFilter")) {
      filter = new RuleFilter(conf)
    }else if (conf.filter_type.equals("DnsFilter")) {
      filter = new DnsFilter(conf)
    }
    filter
  }
}
